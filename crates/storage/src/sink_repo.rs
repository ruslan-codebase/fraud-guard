use async_trait::async_trait;
use fraud_guard_domain::{Decision, DomainError, TransactionRequest};

#[async_trait]
pub trait SinkRepository: Send + Sync {
    async fn insert_transaction_and_decision(
        &self,
        tx: &TransactionRequest,
        decision: &Decision,
    ) -> Result<(), DomainError>;
}
