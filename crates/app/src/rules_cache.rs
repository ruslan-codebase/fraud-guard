use fraud_guard_domain::{Rule, RuleRepository};
use std::sync::Arc;
use tokio::sync::watch;
use tokio::time::{Duration, interval};
use tracing::{error, info};

pub fn start_rules_cache_refresh(
    repo: Arc<dyn RuleRepository>,
    interval_secs: u64,
    rules_tx: watch::Sender<Arc<Vec<Rule>>>,
) {
    tokio::spawn(async move {
        let mut interval = interval(Duration::from_secs(interval_secs));
        loop {
            interval.tick().await;
            match repo.load_active_rules().await {
                Ok(rules) => {
                    let rules_arc = Arc::new(rules);
                    if rules_tx.send(rules_arc).is_err() {
                        error!("Failed to send updated rules");
                        break;
                    }
                    info!("Rules cache refreshed");
                }
                Err(e) => {
                    error!("Failed to load rules: {}", e);
                }
            }
        }
    });
}
