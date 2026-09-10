use crate::actors::messages::UpdateRules;
use fraud_guard_domain::RuleRepository;
use kameo::actor::Recipient;
use std::sync::Arc;
use tokio::time::{Duration, interval};
use tracing::{error, info};

pub fn start_rules_cache_refresh(
    repo: Arc<dyn RuleRepository>,
    interval_secs: u64,
    engine_ref: Recipient<UpdateRules>,
) {
    tokio::spawn(async move {
        let mut interval = interval(Duration::from_secs(interval_secs));
        interval.tick().await;
        loop {
            interval.tick().await;
            match repo.load_active_rules().await {
                Ok(rules) => {
                    let rules_arc = Arc::new(rules);
                    if let Err(e) = engine_ref.tell(UpdateRules { rules: rules_arc }).await {
                        error!("Failed to send updated rules: {}", e);
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
