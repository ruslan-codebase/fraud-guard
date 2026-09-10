mod actors;
mod config;
mod rules_cache;

use crate::actors::messages::{
    AckMessage, EvaluateTransactionRequest, PersistResults, StartConsuming, UpdateRules,
};
use actors::{ConsumerActor, EngineActor, SinkActor};
use config::Config;
use fraud_guard_domain::{RuleRepository, TransactionRepository};
use fraud_guard_storage::postgres::PostgresSinkRepository;
use fraud_guard_storage::postgres::{PostgresRuleRepository, PostgresTransactionRepository};
use fraud_guard_storage::sink_repo::SinkRepository;
use kameo::actor::Spawn;
use std::error;
use std::sync::Arc;
use tokio::main;
use tracing::info;
use tracing_subscriber::{EnvFilter, fmt};

#[main]
async fn main() -> Result<(), Box<dyn error::Error>> {
    let config = Config::from_env()?;

    fmt()
        .with_env_filter(EnvFilter::new(&config.log_level))
        .init();

    let pool = sqlx::PgPool::connect(&config.database_url).await?;

    let tx_repo: Arc<dyn TransactionRepository> =
        Arc::new(PostgresTransactionRepository::new(pool.clone()));
    let rule_repo: Arc<dyn RuleRepository> = Arc::new(PostgresRuleRepository::new(pool.clone()));
    let sink_repo: Arc<dyn SinkRepository> = Arc::new(PostgresSinkRepository::new(pool.clone()));

    let initial_rules = rule_repo.load_active_rules().await?;
    info!("Loaded {} active rules", initial_rules.len());

    let consumer_ref = ConsumerActor::spawn(ConsumerActor::new(
        config.amqp_url.clone(),
        "transaction.evaluation".to_string(),
    ));
    let sink_ref = SinkActor::spawn(SinkActor::new(
        sink_repo.clone(),
        consumer_ref.clone().recipient::<AckMessage>(),
    ));
    let engine_ref = EngineActor::spawn(EngineActor::new(
        tx_repo.clone(),
        sink_ref.clone().recipient::<PersistResults>(),
        Some(initial_rules),
    ));

    consumer_ref
        .tell(StartConsuming {
            engine_ref: engine_ref.clone().recipient::<EvaluateTransactionRequest>(),
        })
        .await?;

    rules_cache::start_rules_cache_refresh(
        rule_repo,
        config.rules_refresh_interval_secs,
        engine_ref.clone().recipient::<UpdateRules>(),
    );

    tokio::signal::ctrl_c()
        .await
        .expect("Failed to listen for shutdown signal");
    info!("Shutting down gracefully...");

    consumer_ref.stop_gracefully().await?;
    consumer_ref.wait_for_shutdown().await;

    engine_ref.stop_gracefully().await?;
    engine_ref.wait_for_shutdown().await;

    sink_ref.stop_gracefully().await?;
    sink_ref.wait_for_shutdown().await;

    info!("Shutdown complete");
    Ok(())
}
