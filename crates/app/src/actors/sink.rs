use crate::actors::messages::{AckMessage, PersistResults};
use fraud_guard_storage::sink_repo::SinkRepository;
use kameo::{
    Actor,
    actor::{ActorRef, Recipient, WeakActorRef},
    error::ActorStopReason,
    message::{Context, Message},
};
use std::sync::Arc;
use tracing::info;

pub struct SinkActor {
    repo: Arc<dyn SinkRepository>,
    consumer_ref: Recipient<AckMessage>,
}

impl SinkActor {
    pub fn new(repo: Arc<dyn SinkRepository>, consumer_ref: Recipient<AckMessage>) -> Self {
        Self { repo, consumer_ref }
    }
}

impl Actor for SinkActor {
    type Args = Self;
    type Error = std::convert::Infallible;

    async fn on_start(state: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        info!("SinkActor started");
        Ok(state)
    }

    async fn on_stop(
        &mut self,
        _actor_ref: WeakActorRef<Self>,
        _reason: ActorStopReason,
    ) -> Result<(), Self::Error> {
        info!("SinkActor stopping gracefully");
        Ok(())
    }
}

impl Message<PersistResults> for SinkActor {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: PersistResults,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let tx = msg.tx;
        let decision = msg.decision;
        let corr_id = msg.correlation_id;

        let result = self
            .repo
            .insert_transaction_and_decision(&tx, &decision)
            .await;
        let _ = self
            .consumer_ref
            .tell(AckMessage {
                correlation_id: corr_id,
                result,
            })
            .await;
    }
}

#[cfg(test)]
mod tests {
    use std::{convert::Infallible, sync::Mutex};

    use super::*;
    use async_trait::async_trait;
    use chrono::Utc;
    use fraud_guard_api::amqp::CorrelationId;
    use fraud_guard_domain::{
        AccountId, Currency, Decision, DomainError, TransactionId, TransactionRequest,
        TriggeredRule,
    };
    use kameo::actor::Spawn;
    use tokio::sync::mpsc;

    #[derive(Default)]
    struct MockSinkRepo {
        inserted: Arc<std::sync::Mutex<Vec<(TransactionRequest, Decision)>>>,
    }

    #[async_trait]
    impl SinkRepository for MockSinkRepo {
        async fn insert_transaction_and_decision(
            &self,
            tx: &TransactionRequest,
            decision: &Decision,
        ) -> Result<(), DomainError> {
            let mut guard = self.inserted.lock().unwrap();
            guard.push((tx.clone(), decision.clone()));
            Ok(())
        }
    }

    struct MockConsumerActor {
        captured: Arc<Mutex<Vec<AckMessage>>>,
        tx: Option<mpsc::UnboundedSender<()>>,
    }

    impl Actor for MockConsumerActor {
        type Args = Self;
        type Error = Infallible;
        async fn on_start(state: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
            Ok(state)
        }
    }

    impl Message<AckMessage> for MockConsumerActor {
        type Reply = ();
        async fn handle(&mut self, msg: AckMessage, _ctx: &mut Context<Self, Self::Reply>) {
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

    #[tokio::test]
    async fn sink_stores_transaction_and_decision() {
        let sink_repo = Arc::new(MockSinkRepo::default());
        let (done_tx, mut done_rx) = mpsc::unbounded_channel();
        let captured = Arc::new(Mutex::new(Vec::new()));
        let consumer_ref = MockConsumerActor::spawn(MockConsumerActor {
            captured: captured.clone(),
            tx: Some(done_tx),
        });

        let sink_ref = SinkActor::spawn(SinkActor::new(
            sink_repo,
            consumer_ref.recipient::<AckMessage>(),
        ));
        let tx = test_transaction(100);
        let corr_id: CorrelationId = 123;
        let triggered = vec![TriggeredRule {
            code: "TEST001".to_string(),
            reason: "Exceeds limit".to_string(),
        }];
        let decision = Decision::Review {
            triggered_rules: triggered,
        };

        sink_ref
            .tell(PersistResults {
                tx: tx.clone(),
                decision,
                correlation_id: corr_id,
            })
            .await
            .unwrap();

        tokio::time::timeout(std::time::Duration::from_secs(1), done_rx.recv())
            .await
            .unwrap()
            .unwrap();

        let msgs = captured.lock().unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].correlation_id, corr_id);
        assert!(msgs[0].result.is_ok());
    }

    #[tokio::test]
    async fn sink_handles_multiple_messages() {
        let sink_repo = Arc::new(MockSinkRepo::default());
        let (done_tx, mut done_rx) = mpsc::unbounded_channel();
        let captured = Arc::new(Mutex::new(Vec::new()));
        let consumer_ref = MockConsumerActor::spawn(MockConsumerActor {
            captured: captured.clone(),
            tx: Some(done_tx),
        });

        let sink_ref = SinkActor::spawn(SinkActor::new(
            sink_repo.clone(),
            consumer_ref.recipient::<AckMessage>(),
        ));

        for i in 0..3 {
            let tx = test_transaction(100 + i);
            let corr_id: CorrelationId = 123 + i as u64;
            let decision = Decision::Accept;
            sink_ref
                .tell(PersistResults {
                    tx: tx.clone(),
                    decision,
                    correlation_id: corr_id,
                })
                .await
                .unwrap();
        }

        tokio::time::timeout(std::time::Duration::from_secs(1), done_rx.recv())
            .await
            .unwrap()
            .unwrap();

        let msgs = captured.lock().unwrap();
        assert_eq!(msgs.len(), 3);
        assert!(msgs[0].result.is_ok());
        assert!(msgs[1].result.is_ok());
        assert!(msgs[2].result.is_ok());

        let inserted = sink_repo.inserted.lock().unwrap();
        assert_eq!(inserted.len(), 3);
    }
}
