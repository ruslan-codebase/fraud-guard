use crate::actors::messages::{EvaluateTransactionRequest, PersistResults, UpdateRules};
use fraud_guard_domain::{Decision, Rule, TransactionRepository, TriggeredRule, Verdict};
use kameo::{
    Actor,
    actor::{ActorRef, Recipient, WeakActorRef},
    error::ActorStopReason,
    message::{Context, Message},
};
use std::sync::Arc;
use tracing::{error, info, trace};

pub struct EngineActor {
    rules: Arc<Vec<Rule>>,
    repo: Arc<dyn TransactionRepository>,
    sink_ref: Recipient<PersistResults>,
}

impl EngineActor {
    pub fn new(
        repo: Arc<dyn TransactionRepository>,
        sink_ref: Recipient<PersistResults>,
        rules: Option<Vec<Rule>>,
    ) -> Self {
        let initial_rules = match rules {
            Some(rules) => Arc::new(rules),
            None => Arc::new(Vec::new()),
        };

        Self {
            rules: initial_rules,
            repo,
            sink_ref,
        }
    }
}

impl Actor for EngineActor {
    type Args = Self;
    type Error = std::convert::Infallible;

    async fn on_start(state: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        info!("EngineActor started");
        Ok(state)
    }

    async fn on_stop(
        &mut self,
        _actor_ref: WeakActorRef<Self>,
        _reason: ActorStopReason,
    ) -> Result<(), Self::Error> {
        info!("EngineActor stopping gracefully");
        Ok(())
    }
}

impl Message<UpdateRules> for EngineActor {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: UpdateRules,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.rules = msg.rules;
        info!("Rules cache updated: {} rules loaded", self.rules.len());
    }
}

impl Message<EvaluateTransactionRequest> for EngineActor {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: EvaluateTransactionRequest,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let tx = msg.tx;
        let corr_id = msg.correlation_id;

        if let Err(e) = tx.validate() {
            error!("Transaction validation failed: {}", e);
            let decision = Decision::Decline {
                reason: e.to_string(),
            };
            let _ = self
                .sink_ref
                .tell(PersistResults {
                    tx,
                    decision,
                    correlation_id: corr_id,
                })
                .await;
            return;
        }

        let mut triggered = Vec::new();

        for rule in self.rules.iter() {
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
                }
            }
        }

        let decision = if triggered.is_empty() {
            Decision::Accept
        } else {
            Decision::Review {
                triggered_rules: triggered,
            }
        };

        if let Err(e) = self
            .sink_ref
            .tell(PersistResults {
                tx,
                decision,
                correlation_id: corr_id,
            })
            .await
        {
            error!("Failed to send decision to sink: {}", e);
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
    use kameo::actor::Spawn;
    use std::{
        convert::Infallible,
        sync::{Arc, Mutex},
    };
    use tokio::sync::mpsc;

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

    struct MockSinkActor {
        captured: Arc<Mutex<Vec<PersistResults>>>,
        tx: Option<mpsc::UnboundedSender<()>>,
    }

    impl Actor for MockSinkActor {
        type Args = Self;
        type Error = Infallible;
        async fn on_start(state: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
            Ok(state)
        }
    }

    impl Message<PersistResults> for MockSinkActor {
        type Reply = ();
        async fn handle(&mut self, msg: PersistResults, _ctx: &mut Context<Self, Self::Reply>) {
            self.captured.lock().unwrap().push(msg);
            if let Some(tx) = &self.tx {
                let _ = tx.send(());
            }
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

    fn invalid_test_transaction(amount: i64) -> TransactionRequest {
        let source = AccountId::new("1111222233334444").unwrap();
        TransactionRequest {
            id: TransactionId::new(),
            source_account: source.clone(),
            destination_account: source,
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
        let (done_tx, mut done_rx) = mpsc::unbounded_channel();
        let captured = Arc::new(Mutex::new(Vec::new()));
        let sink_ref = MockSinkActor::spawn(MockSinkActor {
            captured: captured.clone(),
            tx: Some(done_tx),
        });
        let sink_rec: Recipient<PersistResults> = sink_ref.recipient::<PersistResults>();
        let rules = vec![create_rule(
            RuleType::Threshold,
            serde_json::json!({"max_amount": 10000}),
        )];

        let engine_ref = EngineActor::spawn(EngineActor::new(repo, sink_rec, Some(rules)));
        let tx = test_transaction(100);

        engine_ref
            .tell(EvaluateTransactionRequest {
                tx,
                correlation_id: 123,
            })
            .await
            .unwrap();

        tokio::time::timeout(std::time::Duration::from_secs(1), done_rx.recv())
            .await
            .unwrap()
            .unwrap();

        let msgs = captured.lock().unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].decision, Decision::Accept);
    }

    #[tokio::test]
    async fn engine_triggers_rule() {
        let repo = Arc::new(MockRepo {});
        let (done_tx, mut done_rx) = mpsc::unbounded_channel();
        let captured = Arc::new(Mutex::new(Vec::new()));
        let sink_ref = MockSinkActor::spawn(MockSinkActor {
            captured: captured.clone(),
            tx: Some(done_tx),
        });
        let sink_rec: Recipient<PersistResults> = sink_ref.recipient::<PersistResults>();
        let rules = vec![create_rule(
            RuleType::Threshold,
            serde_json::json!({"max_amount": 50}),
        )];

        let engine_ref = EngineActor::spawn(EngineActor::new(repo, sink_rec, Some(rules)));
        let tx = test_transaction(100);

        engine_ref
            .tell(EvaluateTransactionRequest {
                tx,
                correlation_id: 123,
            })
            .await
            .unwrap();

        tokio::time::timeout(std::time::Duration::from_secs(1), done_rx.recv())
            .await
            .unwrap()
            .unwrap();

        let msgs = captured.lock().unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].tx.amount, 100);
        match msgs[0].decision.clone() {
            Decision::Review { triggered_rules } => {
                assert_eq!(triggered_rules.len(), 1);
                assert_eq!(triggered_rules[0].code, "TEST001");
            }
            _ => panic!("Expected Review, got {:?}", msgs[0].decision),
        }
    }

    #[tokio::test]
    async fn engine_handles_invalid_transaction() {
        let repo = Arc::new(MockRepo {});
        let (done_tx, mut done_rx) = mpsc::unbounded_channel();
        let captured = Arc::new(Mutex::new(Vec::new()));
        let sink_ref = MockSinkActor::spawn(MockSinkActor {
            captured: captured.clone(),
            tx: Some(done_tx),
        });
        let sink_rec: Recipient<PersistResults> = sink_ref.recipient::<PersistResults>();
        let rules = vec![create_rule(
            RuleType::Threshold,
            serde_json::json!({"max_amount": 50}),
        )];

        let engine_ref = EngineActor::spawn(EngineActor::new(repo, sink_rec, Some(rules)));
        let tx = invalid_test_transaction(100);

        engine_ref
            .tell(EvaluateTransactionRequest {
                tx,
                correlation_id: 123,
            })
            .await
            .unwrap();

        tokio::time::timeout(std::time::Duration::from_secs(1), done_rx.recv())
            .await
            .unwrap()
            .unwrap();

        let msgs = captured.lock().unwrap();
        assert_eq!(msgs.len(), 1);
        match msgs[0].decision.clone() {
            Decision::Decline { reason } => {
                assert!(reason.contains("Source and destination accounts must differ"));
            }
            _ => panic!("Expected Decline, got {:?}", msgs[0].decision),
        }
    }

    #[tokio::test]
    async fn engine_updates_rules_cache() {
        let repo = Arc::new(MockRepo {});
        let (done_tx, mut done_rx) = mpsc::unbounded_channel();
        let captured = Arc::new(Mutex::new(Vec::new()));
        let sink_ref = MockSinkActor::spawn(MockSinkActor {
            captured: captured.clone(),
            tx: Some(done_tx),
        });
        let sink_rec: Recipient<PersistResults> = sink_ref.recipient::<PersistResults>();
        let rules = vec![create_rule(
            RuleType::Threshold,
            serde_json::json!({"max_amount": 1000}),
        )];

        let engine_ref = EngineActor::spawn(EngineActor::new(repo, sink_rec, Some(rules)));
        let tx = test_transaction(100);

        engine_ref
            .tell(EvaluateTransactionRequest {
                tx: tx.clone(),
                correlation_id: 123,
            })
            .await
            .unwrap();

        tokio::time::timeout(std::time::Duration::from_secs(1), done_rx.recv())
            .await
            .unwrap()
            .unwrap();

        {
            let msgs = captured.lock().unwrap();
            assert_eq!(msgs.len(), 1);
            assert_eq!(msgs[0].decision, Decision::Accept);
        }

        let rules2 = vec![create_rule(
            RuleType::Threshold,
            serde_json::json!({"max_amount": 10}),
        )];

        engine_ref
            .tell(UpdateRules {
                rules: Arc::new(rules2),
            })
            .await
            .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        engine_ref
            .tell(EvaluateTransactionRequest {
                tx: tx.clone(),
                correlation_id: 456,
            })
            .await
            .unwrap();

        tokio::time::timeout(std::time::Duration::from_secs(1), done_rx.recv())
            .await
            .unwrap()
            .unwrap();

        {
            let msgs2 = captured.lock().unwrap();
            assert_eq!(msgs2.len(), 2);
            match msgs2[1].decision.clone() {
                Decision::Review { triggered_rules } => {
                    assert_eq!(triggered_rules.len(), 1);
                }
                _ => panic!("Expected Review, got {:?}", msgs2[0].decision),
            }
        }
    }
}
