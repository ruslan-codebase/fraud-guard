use crate::{AccountId, DomainError, TransactionRequest};
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
}
