use crate::{AccountId, Decision, DomainError, Rule, TransactionId, TransactionRequest};
use async_trait::async_trait;
use chrono::{DateTime, Utc};

#[async_trait]
pub trait TransactionRepository: Send + Sync {
    async fn get_transactions_for_account(
        &self,
        account: &AccountId,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<Vec<TransactionRequest>, DomainError>;

    async fn insert_transaction(&self, tx_req: &TransactionRequest) -> Result<(), DomainError>;
}

#[async_trait]
pub trait RuleRepository: Send + Sync {
    async fn load_active_rules(&self) -> Result<Vec<Rule>, DomainError>;
}

#[async_trait]
pub trait DecisionRepository: Send + Sync {
    async fn store_decision(
        &self,
        transaction_id: &TransactionId,
        decision: &Decision,
    ) -> Result<(), DomainError>;
}
