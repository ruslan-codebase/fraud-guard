mod actors;
mod config;
mod rules_cache;

use actors::{EngineActor, SinkActor};
use config::Config;
use fraud_guard_domain::DecisionRepository;
use fraud_guard_domain::RuleRepository;
use fraud_guard_domain::TransactionRepository;
use fraud_guard_storage::postgres::{
    PostgresDecisionRepository, PostgresRuleRepository, PostgresTransactionRepository,
};
use rules_cache::start_rules_cache_refresh;

use std::error;
use std::sync::Arc;
use std::time;
use tokio::main;
use tokio::sync::{mpsc, watch};
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
    let decision_repo: Arc<dyn DecisionRepository> =
        Arc::new(PostgresDecisionRepository::new(pool.clone()));

    let rules = rule_repo.load_active_rules().await?;
    let rules_arc = Arc::new(rules);
    tracing::info!("Loaded {} active rules", rules_arc.len());

    let (rules_tx, rules_rx) = watch::channel(rules_arc);
    start_rules_cache_refresh(rule_repo, config.rules_refresh_interval_secs, rules_tx);

    let (_engine_tx, engine_rx) = mpsc::channel(100);
    let (sink_tx, sink_rx) = mpsc::channel(100);

    let engine = EngineActor {
        rules_rx,
        repo: tx_repo.clone(),
        rx: engine_rx,
        sink_tx: sink_tx.clone(),
    };
    tokio::spawn(engine.run());

    let sink = SinkActor {
        tx_repo: tx_repo.clone(),
        decision_repo: decision_repo.clone(),
        rx: sink_rx,
    };
    tokio::spawn(sink.run());

    tokio::signal::ctrl_c()
        .await
        .expect("Failed to listen for shutdown signal");
    info!("Shutting down gracefully...");

    tokio::time::sleep(time::Duration::from_secs(2)).await;
    Ok(())
}
