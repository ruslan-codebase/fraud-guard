use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use fraud_guard_api::amqp::CorrelationId;
use fraud_guard_app::actors::messages::{AckMessage, EvaluateTransactionRequest, PersistResults};
use fraud_guard_app::actors::{EngineActor, SinkActor};
use fraud_guard_domain::{
    AccountId, Currency, Decision, DomainError, Rule, RuleType, TransactionId,
    TransactionRepository, TransactionRequest,
};
use fraud_guard_storage::sink_repo::SinkRepository;
use kameo::actor::{Actor, ActorRef, Spawn, WeakActorRef};
use kameo::error::ActorStopReason;
use kameo::message::{Context, Message};
use std::convert::Infallible;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;

#[derive(Default)]
struct MockTxRepo;

#[async_trait]
impl TransactionRepository for MockTxRepo {
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
    inserted: Arc<Mutex<Vec<(TransactionRequest, Decision)>>>,
}

#[async_trait]
impl SinkRepository for MockSinkRepo {
    async fn insert_transaction_and_decision(
        &self,
        tx: &TransactionRequest,
        decision: &Decision,
    ) -> Result<(), DomainError> {
        self.inserted
            .lock()
            .unwrap()
            .push((tx.clone(), decision.clone()));
        Ok(())
    }
}

struct MockConsumerActor {
    captured: Arc<Mutex<Vec<AckMessage>>>,
    notify: Option<mpsc::UnboundedSender<()>>,
}

impl Actor for MockConsumerActor {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }

    async fn on_stop(
        &mut self,
        _: WeakActorRef<Self>,
        _: ActorStopReason,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

impl Message<AckMessage> for MockConsumerActor {
    type Reply = ();

    async fn handle(&mut self, msg: AckMessage, _ctx: &mut Context<Self, Self::Reply>) {
        self.captured.lock().unwrap().push(msg);
        if let Some(tx) = &self.notify {
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

fn create_rule(rule_type: RuleType, params: serde_json::Value) -> Rule {
    Rule {
        code: "TEST001".to_string(),
        rule_type,
        params,
        enabled: true,
        priority: 10,
        valid_from: Utc::now() - ChronoDuration::days(1),
        valid_until: Some(Utc::now() + ChronoDuration::days(1)),
    }
}

#[tokio::test]
async fn pipeline_accepts_clean_transaction() {
    let tx_repo = Arc::new(MockTxRepo);
    let sink_repo = Arc::new(MockSinkRepo::default());

    let captured = Arc::new(Mutex::new(Vec::new()));
    let (notify_tx, mut notify_rx) = mpsc::unbounded_channel();

    let consumer_ref = MockConsumerActor::spawn(MockConsumerActor {
        captured: captured.clone(),
        notify: Some(notify_tx),
    });

    let sink_ref = SinkActor::spawn(SinkActor::new(
        sink_repo.clone(),
        consumer_ref.recipient::<AckMessage>(),
    ));

    let rules = vec![create_rule(
        RuleType::Threshold,
        serde_json::json!({"max_amount": 10000}),
    )];
    let engine_ref = EngineActor::spawn(EngineActor::new(
        tx_repo.clone(),
        sink_ref.recipient::<PersistResults>(),
        Some(rules),
    ));

    let corr_id: CorrelationId = 42;
    engine_ref
        .tell(EvaluateTransactionRequest {
            tx: test_transaction(100),
            correlation_id: corr_id,
        })
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(1), notify_rx.recv())
        .await
        .expect("timeout waiting for ack")
        .unwrap();

    let inserted = sink_repo.inserted.lock().unwrap();
    assert_eq!(inserted.len(), 1);
    assert_eq!(inserted[0].1, Decision::Accept);

    let acks = captured.lock().unwrap();
    assert_eq!(acks.len(), 1);
    assert_eq!(acks[0].correlation_id, corr_id);
    assert!(acks[0].result.is_ok());
}

#[tokio::test]
async fn pipeline_triggers_rule_and_reviews() {
    let tx_repo = Arc::new(MockTxRepo);
    let sink_repo = Arc::new(MockSinkRepo::default());

    let captured = Arc::new(Mutex::new(Vec::new()));
    let (notify_tx, mut notify_rx) = mpsc::unbounded_channel();

    let consumer_ref = MockConsumerActor::spawn(MockConsumerActor {
        captured: captured.clone(),
        notify: Some(notify_tx),
    });

    let sink_ref = SinkActor::spawn(SinkActor::new(
        sink_repo.clone(),
        consumer_ref.recipient::<AckMessage>(),
    ));

    let rules = vec![create_rule(
        RuleType::Threshold,
        serde_json::json!({"max_amount": 50}),
    )];
    let engine_ref = EngineActor::spawn(EngineActor::new(
        tx_repo.clone(),
        sink_ref.recipient::<PersistResults>(),
        Some(rules),
    ));

    let corr_id: CorrelationId = 99;
    engine_ref
        .tell(EvaluateTransactionRequest {
            tx: test_transaction(100),
            correlation_id: corr_id,
        })
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(1), notify_rx.recv())
        .await
        .expect("timeout waiting for ack")
        .unwrap();

    let inserted = sink_repo.inserted.lock().unwrap();
    assert_eq!(inserted.len(), 1);
    match &inserted[0].1 {
        Decision::Review { triggered_rules } => {
            assert_eq!(triggered_rules.len(), 1);
            assert_eq!(triggered_rules[0].code, "TEST001");
        }
        other => panic!("Expected Review, got {:?}", other),
    }

    let acks = captured.lock().unwrap();
    assert_eq!(acks.len(), 1);
    assert_eq!(acks[0].correlation_id, corr_id);
    assert!(acks[0].result.is_ok());
}

#[tokio::test]
async fn pipeline_declines_invalid_transaction() {
    let tx_repo = Arc::new(MockTxRepo);
    let sink_repo = Arc::new(MockSinkRepo::default());

    let captured = Arc::new(Mutex::new(Vec::new()));
    let (notify_tx, mut notify_rx) = mpsc::unbounded_channel();

    let consumer_ref = MockConsumerActor::spawn(MockConsumerActor {
        captured: captured.clone(),
        notify: Some(notify_tx),
    });

    let sink_ref = SinkActor::spawn(SinkActor::new(
        sink_repo.clone(),
        consumer_ref.recipient::<AckMessage>(),
    ));

    let engine_ref = EngineActor::spawn(EngineActor::new(
        tx_repo.clone(),
        sink_ref.recipient::<PersistResults>(),
        None,
    ));

    let source = AccountId::new("1111222233334444").unwrap();
    let tx = TransactionRequest {
        id: TransactionId::new(),
        source_account: source.clone(),
        destination_account: source,
        amount: 100,
        currency: Currency::Rub,
        timestamp: Utc::now(),
    };

    let corr_id: CorrelationId = 7;
    engine_ref
        .tell(EvaluateTransactionRequest {
            tx,
            correlation_id: corr_id,
        })
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(1), notify_rx.recv())
        .await
        .expect("timeout waiting for ack")
        .unwrap();

    let inserted = sink_repo.inserted.lock().unwrap();
    assert_eq!(inserted.len(), 1);
    match &inserted[0].1 {
        Decision::Decline { reason } => {
            assert!(reason.contains("Source and destination accounts must differ"));
        }
        other => panic!("Expected Decline, got {:?}", other),
    }

    let acks = captured.lock().unwrap();
    assert_eq!(acks.len(), 1);
    assert!(acks[0].result.is_ok());
}
