mod config;

use config::Config;
use fraud_guard_domain::RuleRepository;
use fraud_guard_storage::postgres::{
    PostgresDecisionRepository, PostgresRuleRepository, PostgresTransactionRepository,
};

use std::error;
use std::time;
use tokio::main;
use tracing_subscriber::{EnvFilter, fmt};

#[main]
async fn main() -> Result<(), Box<dyn error::Error>> {
    let config = Config::from_env()?;

    fmt()
        .with_env_filter(EnvFilter::new(&config.log_level))
        .init();

    let pool = sqlx::PgPool::connect(&config.database_url).await?;

    let _ = PostgresTransactionRepository::new(pool.clone());
    let rule_repo = PostgresRuleRepository::new(pool.clone());
    let _ = PostgresDecisionRepository::new(pool.clone());

    let rules = rule_repo.load_active_rules().await?;
    tracing::info!("Loaded {} active rules", rules.len());

    // TODO: setup actor based processing pipeline

    tokio::time::sleep(time::Duration::from_secs(2)).await;
    Ok(())
}
