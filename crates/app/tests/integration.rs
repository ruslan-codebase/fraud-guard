use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use fraud_guard_api::amqp::CorrelationId;
use fraud_guard_app::actors::{EngineActor, SinkActor};
use fraud_guard_domain::{
    AccountId, Currency, Decision, DomainError, Rule, RuleType, TransactionId,
    TransactionRepository, TransactionRequest,
};
use fraud_guard_storage::sink_repo::SinkRepository;
use std::sync::Arc;
use tokio::sync::{mpsc, watch};

#[derive(Default)]
struct MockTransactionRepo;

#[async_trait]
impl TransactionRepository for MockTransactionRepo {
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

#[derive(Default)]
struct MockSinkRepo {
    stored: Arc<std::sync::Mutex<Vec<(TransactionRequest, Decision)>>>,
}

#[async_trait]
impl SinkRepository for MockSinkRepo {
    async fn insert_transaction_and_decision(
        &self,
        tx: &TransactionRequest,
        decision: &Decision,
    ) -> Result<(), DomainError> {
        let mut guard = self.stored.lock().unwrap();
        guard.push((tx.clone(), decision.clone()));
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
async fn actors_pipeline_accepts_clean_transaction() {
    let tx_repo: Arc<dyn TransactionRepository> = Arc::new(MockTransactionRepo);
    let sink_repo = Arc::new(MockSinkRepo::default());

    let rules = vec![create_rule(
        RuleType::Threshold,
        serde_json::json!({"max_amount": 10000}),
    )];
    let rules_arc = Arc::new(rules);
    let (_rules_tx, rules_rx) = watch::channel(rules_arc);
    let (shutdown_tx, shutdown_rx) = watch::channel(());

    let (engine_tx, engine_rx) = mpsc::channel::<(TransactionRequest, CorrelationId)>(10);
    let (sink_tx, sink_rx) = mpsc::channel::<(TransactionRequest, Decision, CorrelationId)>(10);
    let (ack_tx, mut ack_rx) = mpsc::channel::<(CorrelationId, Result<(), DomainError>)>(10);

    let engine = EngineActor {
        rules_rx,
        repo: tx_repo.clone(),
        engine_rx,
        sink_tx: sink_tx.clone(),
        shutdown_rx: shutdown_rx.clone(),
    };
    let engine_handle = tokio::spawn(engine.run());

    let sink = SinkActor {
        sink_repo: sink_repo.clone(),
        sink_rx,
        ack_tx,
        shutdown_rx: shutdown_rx.clone(),
    };
    let sink_handle = tokio::spawn(sink.run());

    let tx = test_transaction(100);
    let corr_id: CorrelationId = 123;
    engine_tx.send((tx, corr_id)).await.unwrap();

    let (ack_corr_id, result) =
        tokio::time::timeout(std::time::Duration::from_secs(5), ack_rx.recv())
            .await
            .expect("Timeout waiting for ack")
            .expect("Ack channel closed");
    assert_eq!(ack_corr_id, corr_id);
    assert!(result.is_ok());

    {
        let stored = sink_repo.stored.lock().unwrap();
        assert_eq!(stored.len(), 1);
        let (stored_tx, stored_decision) = &stored[0];
        assert_eq!(stored_tx.amount, 100);
        assert_eq!(*stored_decision, Decision::Accept);
    }

    let _ = shutdown_tx.send(());

    drop(engine_tx);
    drop(sink_tx);

    let _ = tokio::time::timeout(std::time::Duration::from_secs(2), engine_handle).await;
    let _ = tokio::time::timeout(std::time::Duration::from_secs(2), sink_handle).await;
}

#[tokio::test]
async fn engine_to_sink_pipeline_triggers_rule() {
    let tx_repo: Arc<dyn TransactionRepository> = Arc::new(MockTransactionRepo);
    let sink_repo = Arc::new(MockSinkRepo::default());

    let rules = vec![create_rule(
        RuleType::Threshold,
        serde_json::json!({"max_amount": 50}),
    )];
    let rules_arc = Arc::new(rules);
    let (_rules_tx, rules_rx) = watch::channel(rules_arc);
    let (shutdown_tx, shutdown_rx) = watch::channel(());

    let (engine_tx, engine_rx) = mpsc::channel::<(TransactionRequest, CorrelationId)>(10);
    let (sink_tx, sink_rx) = mpsc::channel::<(TransactionRequest, Decision, CorrelationId)>(10);
    let (ack_tx, mut ack_rx) = mpsc::channel::<(CorrelationId, Result<(), DomainError>)>(10);

    let engine = EngineActor {
        rules_rx,
        repo: tx_repo.clone(),
        engine_rx,
        sink_tx: sink_tx.clone(),
        shutdown_rx: shutdown_rx.clone(),
    };
    let engine_handle = tokio::spawn(engine.run());

    let sink = SinkActor {
        sink_repo: sink_repo.clone(),
        sink_rx,
        ack_tx,
        shutdown_rx: shutdown_rx.clone(),
    };
    let sink_handle = tokio::spawn(sink.run());

    let tx = test_transaction(100);
    let corr_id: CorrelationId = 456;
    engine_tx.send((tx, corr_id)).await.unwrap();

    let (ack_corr_id, result) = ack_rx.recv().await.unwrap();
    assert_eq!(ack_corr_id, corr_id);
    assert!(result.is_ok());

    {
        let stored = sink_repo.stored.lock().unwrap();
        assert_eq!(stored.len(), 1);
        let (_stored_tx, stored_decision) = &stored[0];
        match stored_decision {
            Decision::Review { triggered_rules } => {
                assert_eq!(triggered_rules.len(), 1);
                assert_eq!(triggered_rules[0].code, "TEST001");
            }
            _ => panic!("Expected Review decision"),
        }
    }

    let _ = shutdown_tx.send(());

    drop(engine_tx);
    drop(sink_tx);

    let _ = tokio::time::timeout(std::time::Duration::from_secs(2), engine_handle).await;
    let _ = tokio::time::timeout(std::time::Duration::from_secs(2), sink_handle).await;
}
