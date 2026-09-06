use fraud_guard_api::amqp::CorrelationId;
use fraud_guard_domain::{Decision, DomainError, TransactionRequest};
use fraud_guard_storage::sink_repo::SinkRepository;
use std::sync::Arc;
use tokio::sync::{mpsc, watch};
use tracing::{error, info};

pub struct SinkActor {
    // pub tx_repo: Arc<dyn TransactionRepository>,
    // pub decision_repo: Arc<dyn DecisionRepository>,
    pub sink_repo: Arc<dyn SinkRepository>,
    pub rx: mpsc::Receiver<(TransactionRequest, Decision, CorrelationId)>,
    pub ack_tx: mpsc::Sender<(CorrelationId, Result<(), DomainError>)>,
    pub shutdown_rx: watch::Receiver<()>,
}

impl SinkActor {
    pub async fn run(mut self) {
        loop {
            tokio::select! {
                _ = self.shutdown_rx.changed() => {
                    info!("Shutdown signal received, exiting sink");
                    break;
                }
                Some((tx, decision, corr_id)) = self.rx.recv() => {
                    let result = self.sink_repo.insert_transaction_and_decision(&tx, &decision).await;
                    if let Err(e) = self.ack_tx.send((corr_id, result)).await {
                        error!("Failed to send ack result for {}: {}", corr_id, e);
                    } else {
                        info!("Transaction {} persisted with decision {:?}", tx.id, decision);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use chrono::Utc;
    use fraud_guard_domain::{
        AccountId, Currency, Decision, DomainError, TransactionId, TransactionRequest,
        TriggeredRule,
    };
    use std::sync::Arc;
    use tokio::sync::mpsc;
    use tokio::time::{Duration, sleep};

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

        let (sink_tx, sink_rx) = mpsc::channel(10);
        let (ack_tx, _) = mpsc::channel(10);
        let (_shutdown_tx, shutdown_rx) = watch::channel(());

        let sink_actor = SinkActor {
            sink_repo: sink_repo.clone(),
            rx: sink_rx,
            ack_tx,
            shutdown_rx,
        };

        tokio::spawn(sink_actor.run());

        let tx = test_transaction(100);
        let corr_id: CorrelationId = 123;
        let triggered = vec![TriggeredRule {
            code: "TEST001".to_string(),
            reason: "Exceeds limit".to_string(),
        }];
        let decision = Decision::Review {
            triggered_rules: triggered,
        };

        sink_tx
            .send((tx.clone(), decision.clone(), corr_id))
            .await
            .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let inserted = sink_repo.inserted.lock().unwrap();
        assert_eq!(inserted.len(), 1);
        let (inserted_tx, inserted_decision) = &inserted[0];
        assert_eq!(inserted_tx.id, tx.id);
        assert_eq!(*inserted_decision, decision);
    }

    #[tokio::test]
    async fn sink_handles_multiple_messages() {
        let sink_repo = Arc::new(MockSinkRepo::default());

        let (sink_tx, sink_rx) = mpsc::channel(10);
        let (ack_tx, _) = mpsc::channel(10);
        let (_shutdown_tx, shutdown_rx) = watch::channel(());

        let sink_actor = SinkActor {
            sink_repo: sink_repo.clone(),
            rx: sink_rx,
            ack_tx,
            shutdown_rx,
        };

        tokio::spawn(sink_actor.run());

        for i in 0..3 {
            let tx = test_transaction(100 + i);
            let corr_id: CorrelationId = 123 + i as u64;
            let decision = Decision::Accept;
            sink_tx.send((tx, decision, corr_id)).await.unwrap();
        }

        drop(sink_tx);

        let timeout = Duration::from_secs(5);
        let start = std::time::Instant::now();
        while start.elapsed() < timeout {
            let inserted_count = sink_repo.inserted.lock().unwrap().len();
            if inserted_count == 3 {
                break;
            }
            sleep(Duration::from_millis(50)).await
        }

        let inserted_count = sink_repo.inserted.lock().unwrap().len();
        assert_eq!(inserted_count, 3);
    }
}
