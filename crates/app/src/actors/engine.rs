use fraud_guard_api::amqp::CorrelationId;
use fraud_guard_domain::{
    Decision, Rule, TransactionRepository, TransactionRequest, TriggeredRule, Verdict,
};
use std::sync::Arc;
use tokio::sync::{mpsc, watch};
use tracing::{error, info, trace};

pub struct EngineActor {
    pub rules_rx: watch::Receiver<Arc<Vec<Rule>>>,
    pub repo: Arc<dyn TransactionRepository>,
    pub rx: mpsc::Receiver<(TransactionRequest, CorrelationId)>,
    pub sink_tx: mpsc::Sender<(TransactionRequest, Decision, CorrelationId)>,
    pub shutdown_rx: watch::Receiver<()>,
}

impl EngineActor {
    pub async fn run(mut self) {
        let mut rules = self.rules_rx.borrow().clone();

        loop {
            tokio::select! {
                _ = self.shutdown_rx.changed() => {
                    info!("Shutdown signal received, exiting engine");
                    break;
                }
                Some((tx, corr_id)) = self.rx.recv() => {
                    if let Err(e) = tx.validate() {
                        error!("Transaction validation failed: {}", e);
                        let _ = self.sink_tx.send((tx, Decision::Decline { reason: e.to_string() }, corr_id)).await;
                        continue;
                    }

                    let mut triggered = Vec::new();

                    for rule in rules.iter() {
                        match rule.evaluate(&tx, self.repo.as_ref()).await {
                            Ok(verdict) => match verdict {
                                Verdict::Triggered { code, reason } => {
                                    triggered.push(TriggeredRule { code, reason });
                                }
                                Verdict::Skipped { reason } => {
                                    trace!("Rule {} skipped: {}", rule.code, reason);
                                }
                                Verdict::Passed => {
                                    trace!("Rule {} passed", rule.code);
                                }
                            },
                            Err(e) => {
                                error!("Rule evaluation error: {}", e);
                                // TODO: maybe consider this a trigger?
                            }
                        }
                    }

                    let decision = if triggered.is_empty() {
                        Decision::Accept
                    } else {
                        Decision::Review { triggered_rules: triggered }
                    };

                    if let Err(e) = self.sink_tx.send((tx, decision, corr_id)).await {
                        error!("Failed to send decision to sink: {}", e);
                    }
                }
                Ok(_) = self.rules_rx.changed() => {
                    rules = self.rules_rx.borrow_and_update().clone();
                    info!("Rules cache updated: {} rules loaded", rules.len());
                }
                else => break,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use chrono::{DateTime, Duration, Utc};
    use fraud_guard_domain::{
        AccountId, Currency, Decision, DomainError, RuleType, TransactionId, TransactionRepository,
        TransactionRequest,
    };
    use std::sync::Arc;
    use tokio::sync::{mpsc, watch};

    struct MockRepo {}

    #[async_trait]
    impl TransactionRepository for MockRepo {
        async fn get_transactions_for_account(
            &self,
            _account: &AccountId,
            _from: DateTime<Utc>,
            _to: DateTime<Utc>,
        ) -> Result<Vec<TransactionRequest>, DomainError> {
            Ok(vec![])
        }

        async fn insert_transaction(&self, _tx: &TransactionRequest) -> Result<(), DomainError> {
            Ok(())
        }
    }

    fn test_transaction(amount: i64) -> TransactionRequest {
        let source = AccountId::new("1111222233334444").unwrap();
        let dest = AccountId::new("5555666677778888").unwrap();
        TransactionRequest {
            id: TransactionId::new(),
            source_account: source,
            destination_account: dest,
            amount,
            currency: Currency::Rub,
            timestamp: Utc::now(),
        }
    }

    fn create_rule(rule_type: RuleType, params: serde_json::Value) -> Rule {
        Rule {
            code: "TEST001".to_string(),
            rule_type,
            params,
            enabled: true,
            priority: 10,
            valid_from: Utc::now() - Duration::days(1),
            valid_until: Some(Utc::now() + Duration::days(1)),
        }
    }

    #[tokio::test]
    async fn engine_accepts_clean_transaction() {
        let repo = Arc::new(MockRepo {});
        let (tx_tx, rx_tx) = mpsc::channel(10);
        let (sink_tx, mut sink_rx) = mpsc::channel(10);

        let rules = vec![create_rule(
            RuleType::Threshold,
            serde_json::json!({"max_amount": 10000}),
        )];
        let rules_arc = Arc::new(rules);
        let (_rules_tx, rules_rx) = watch::channel(rules_arc);
        let (_shutdown_tx, shutdown_rx) = watch::channel(());

        let engine = EngineActor {
            rules_rx,
            repo: repo.clone(),
            rx: rx_tx,
            sink_tx: sink_tx.clone(),
            shutdown_rx,
        };

        tokio::spawn(engine.run());

        let tx = test_transaction(100);
        let corr_id: CorrelationId = 123;
        tx_tx.send((tx, corr_id)).await.unwrap();

        let result = tokio::time::timeout(std::time::Duration::from_secs(5), sink_rx.recv()).await;
        let (received_tx, decision, _) = result.unwrap().unwrap();
        assert_eq!(received_tx.amount, 100);
        assert_eq!(decision, Decision::Accept);

        assert!(sink_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn engine_triggers_rule() {
        let repo = Arc::new(MockRepo {});
        let (tx_tx, rx_tx) = mpsc::channel(10);
        let (sink_tx, mut sink_rx) = mpsc::channel(10);

        let rules = vec![create_rule(
            RuleType::Threshold,
            serde_json::json!({"max_amount": 50}),
        )];
        let rules_arc = Arc::new(rules);
        let (_rules_tx, rules_rx) = watch::channel(rules_arc);
        let (_shutdown_tx, shutdown_rx) = watch::channel(());

        let engine = EngineActor {
            rules_rx,
            repo: repo.clone(),
            rx: rx_tx,
            sink_tx: sink_tx.clone(),
            shutdown_rx,
        };

        tokio::spawn(engine.run());

        let tx = test_transaction(100);
        let corr_id: CorrelationId = 123;
        tx_tx.send((tx, corr_id)).await.unwrap();

        let result = tokio::time::timeout(std::time::Duration::from_secs(5), sink_rx.recv()).await;
        let (received_tx, decision, _) = result.unwrap().unwrap();
        assert_eq!(received_tx.amount, 100);
        match decision {
            Decision::Review { triggered_rules } => {
                assert_eq!(triggered_rules.len(), 1);
                assert_eq!(triggered_rules[0].code, "TEST001");
            }
            _ => panic!("Expected Review, got {:?}", decision),
        }
    }

    #[tokio::test]
    async fn engine_handles_invalid_transaction() {
        let repo = Arc::new(MockRepo {});
        let (tx_tx, rx_tx) = mpsc::channel(10);
        let (sink_tx, mut sink_rx) = mpsc::channel(10);

        let rules: Vec<Rule> = vec![];
        let rules_arc = Arc::new(rules);
        let (_rules_tx, rules_rx) = watch::channel(rules_arc);
        let (_shutdown_tx, shutdown_rx) = watch::channel(());

        let engine = EngineActor {
            rules_rx,
            repo: repo.clone(),
            rx: rx_tx,
            sink_tx: sink_tx.clone(),
            shutdown_rx,
        };

        tokio::spawn(engine.run());

        let source = AccountId::new("1111222233334444").unwrap();
        let tx = TransactionRequest {
            id: TransactionId::new(),
            source_account: source.clone(),
            destination_account: source,
            amount: 100,
            currency: Currency::Rub,
            timestamp: Utc::now(),
        };
        let corr_id: CorrelationId = 123;
        tx_tx.send((tx, corr_id)).await.unwrap();

        let result = tokio::time::timeout(std::time::Duration::from_secs(5), sink_rx.recv()).await;
        let (_, decision, _) = result.unwrap().unwrap();
        match decision {
            Decision::Decline { reason } => {
                assert!(reason.contains("Source and destination accounts must differ"));
            }
            _ => panic!("Expected Decline, got {:?}", decision),
        }
    }

    #[tokio::test]
    async fn engine_updates_rules_cache() {
        let repo = Arc::new(MockRepo {});
        let (tx_tx, rx_tx) = mpsc::channel(10);
        let (sink_tx, mut sink_rx) = mpsc::channel(10);

        let rules1 = vec![create_rule(
            RuleType::Threshold,
            serde_json::json!({"max_amount": 100}),
        )];
        let rules_arc1 = Arc::new(rules1);
        let (rules_tx, rules_rx) = watch::channel(rules_arc1);
        let (_shutdown_tx, shutdown_rx) = watch::channel(());

        let engine = EngineActor {
            rules_rx,
            repo: repo.clone(),
            rx: rx_tx,
            sink_tx: sink_tx.clone(),
            shutdown_rx,
        };

        tokio::spawn(engine.run());

        let tx1 = test_transaction(50);
        let corr_id1: CorrelationId = 123;
        tx_tx.send((tx1, corr_id1)).await.unwrap();
        let result1 = tokio::time::timeout(std::time::Duration::from_secs(5), sink_rx.recv()).await;
        let (_, decision1, _) = result1.unwrap().unwrap();
        assert_eq!(decision1, Decision::Accept);

        let rules2 = vec![create_rule(
            RuleType::Threshold,
            serde_json::json!({"max_amount": 10}),
        )];
        let rules_arc2 = Arc::new(rules2);
        rules_tx.send(rules_arc2).unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let tx2 = test_transaction(50);
        let corr_id2: CorrelationId = 456;
        tx_tx.send((tx2, corr_id2)).await.unwrap();
        let result2 = tokio::time::timeout(std::time::Duration::from_secs(5), sink_rx.recv()).await;
        let (_, decision2, _) = result2.unwrap().unwrap();
        match decision2 {
            Decision::Review { triggered_rules } => {
                assert_eq!(triggered_rules.len(), 1);
            }
            _ => panic!("Expected Review, got {:?}", decision2),
        }
    }
}
