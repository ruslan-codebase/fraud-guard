use fraud_guard_api::amqp::CorrelationId;
use fraud_guard_domain::{
    Decision, DecisionRepository, DomainError, TransactionRepository, TransactionRequest,
};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{error, info};

pub struct SinkActor {
    pub tx_repo: Arc<dyn TransactionRepository>,
    pub decision_repo: Arc<dyn DecisionRepository>,
    pub rx: mpsc::Receiver<(TransactionRequest, Decision, CorrelationId)>,
    pub ack_tx: mpsc::Sender<(CorrelationId, Result<(), DomainError>)>,
}

impl SinkActor {
    pub async fn run(mut self) {
        while let Some((tx, decision, corr_id)) = self.rx.recv().await {
            let result = self.insert_transaction_and_decision(&tx, &decision).await;
            if let Err(e) = self.ack_tx.send((corr_id, result)).await {
                error!("Failed to send ack result for {}: {}", corr_id, e);
            } else {
                info!(
                    "Transaction {} persisted with decision {:?}",
                    tx.id, decision
                );
            }
        }
    }

    async fn insert_transaction_and_decision(
        &self,
        tx_req: &TransactionRequest,
        decision: &Decision,
    ) -> Result<(), DomainError> {
        //TODO: probably better to use atomic transaction
        self.tx_repo.insert_transaction(tx_req).await?;
        self.decision_repo
            .store_decision(&tx_req.id, decision)
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use chrono::Utc;
    use fraud_guard_domain::{
        AccountId, Currency, Decision, DecisionRepository, DomainError, TransactionId,
        TransactionRepository, TransactionRequest, TriggeredRule,
    };
    use std::sync::Arc;
    use tokio::sync::mpsc;
    use tokio::time::{Duration, sleep};

    #[derive(Default)]
    struct MockTxRepo {
        inserted: Arc<std::sync::Mutex<Vec<TransactionRequest>>>,
    }

    #[async_trait]
    impl TransactionRepository for MockTxRepo {
        async fn get_transactions_for_account(
            &self,
            _account: &AccountId,
            _from: chrono::DateTime<Utc>,
            _to: chrono::DateTime<Utc>,
        ) -> Result<Vec<TransactionRequest>, DomainError> {
            Ok(vec![])
        }

        async fn insert_transaction(&self, tx: &TransactionRequest) -> Result<(), DomainError> {
            let mut guard = self.inserted.lock().unwrap();
            guard.push(tx.clone());
            Ok(())
        }
    }

    #[derive(Default)]
    struct MockDecisionRepo {
        stored: Arc<std::sync::Mutex<Vec<(TransactionId, Decision)>>>,
    }

    #[async_trait]
    impl DecisionRepository for MockDecisionRepo {
        async fn store_decision(
            &self,
            transaction_id: &TransactionId,
            decision: &Decision,
        ) -> Result<(), DomainError> {
            let mut guard = self.stored.lock().unwrap();
            guard.push((*transaction_id, decision.clone()));
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
        let tx_repo = Arc::new(MockTxRepo::default());
        let decision_repo = Arc::new(MockDecisionRepo::default());

        let (sink_tx, sink_rx) = mpsc::channel(10);
        let (ack_tx, _) = mpsc::channel(10);

        let sink_actor = SinkActor {
            tx_repo: tx_repo.clone(),
            decision_repo: decision_repo.clone(),
            rx: sink_rx,
            ack_tx,
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

        let inserted_txs = tx_repo.inserted.lock().unwrap();
        assert_eq!(inserted_txs.len(), 1);
        assert_eq!(inserted_txs[0].id, tx.id);

        let stored_decisions = decision_repo.stored.lock().unwrap();
        assert_eq!(stored_decisions.len(), 1);
        let (stored_tx_id, stored_decision) = &stored_decisions[0];
        assert_eq!(*stored_tx_id, tx.id);
        assert_eq!(*stored_decision, decision);
    }

    #[tokio::test]
    async fn sink_handles_multiple_messages() {
        let tx_repo = Arc::new(MockTxRepo::default());
        let decision_repo = Arc::new(MockDecisionRepo::default());

        let (sink_tx, sink_rx) = mpsc::channel(10);
        let (ack_tx, _) = mpsc::channel(10);

        let sink_actor = SinkActor {
            tx_repo: tx_repo.clone(),
            decision_repo: decision_repo.clone(),
            rx: sink_rx,
            ack_tx,
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
            let inserted_count = tx_repo.inserted.lock().unwrap().len();
            let stored_count = decision_repo.stored.lock().unwrap().len();
            if inserted_count == 3 && stored_count == 3 {
                break;
            }
            sleep(Duration::from_millis(50)).await
        }

        let inserted_txs = tx_repo.inserted.lock().unwrap();
        assert_eq!(inserted_txs.len(), 3);

        let stored_decisions = decision_repo.stored.lock().unwrap();
        assert_eq!(stored_decisions.len(), 3);
    }
}
