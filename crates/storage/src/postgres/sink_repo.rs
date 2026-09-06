use async_trait::async_trait;
use fraud_guard_domain::{Decision, DomainError, TransactionRequest};
use sqlx::PgPool;

use crate::sink_repo::SinkRepository;

pub struct PostgresSinkRepository {
    pool: PgPool,
}

impl PostgresSinkRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl SinkRepository for PostgresSinkRepository {
    async fn insert_transaction_and_decision(
        &self,
        tx: &TransactionRequest,
        decision: &Decision,
    ) -> Result<(), DomainError> {
        let mut dbtransaction = self
            .pool
            .begin()
            .await
            .map_err(|e| DomainError::Repository {
                operation: "begin db transaction",
                message: e.to_string(),
            })?;

        sqlx::query!(
            r#"
            INSERT INTO transactions (transaction_id, source_account, destination_account, amount, currency, timestamp)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (transaction_id) DO NOTHING
            "#,
            tx.id.as_uuid(),
            tx.source_account.as_str(),
            tx.destination_account.as_str(),
            tx.amount,
            tx.currency.to_string(),
            tx.timestamp,
        )
        .execute(&mut *dbtransaction)
        .await
        .map_err(|e| DomainError::Repository {
            operation: "insert transaction",
            message: e.to_string(),
        })?;

        let decision_json =
            serde_json::to_value(decision).map_err(|e| DomainError::Repository {
                operation: "serialize decision to JSON",
                message: e.to_string(),
            })?;

        sqlx::query!(
            r#"
            INSERT INTO decisions (transaction_id, decision)
            VALUES ($1, $2)
            ON CONFLICT (transaction_id) DO NOTHING
            "#,
            tx.id.as_uuid(),
            decision_json,
        )
        .execute(&mut *dbtransaction)
        .await
        .map_err(|e| DomainError::Repository {
            operation: "insert decision",
            message: e.to_string(),
        })?;

        dbtransaction
            .commit()
            .await
            .map_err(|e| DomainError::Repository {
                operation: "commit db transaction",
                message: e.to_string(),
            })?;

        Ok(())
    }
}
