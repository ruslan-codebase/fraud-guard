use async_trait::async_trait;
use fraud_guard_domain::{Decision, DecisionRepository, DomainError, TransactionId};
use sqlx::PgPool;

pub struct PostgresDecisionRepository {
    pool: PgPool,
}

impl PostgresDecisionRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl DecisionRepository for PostgresDecisionRepository {
    async fn store_decision(
        &self,
        transaction_id: &TransactionId,
        decision: &Decision,
    ) -> Result<(), DomainError> {
        let decision_json =
            serde_json::to_value(decision).map_err(|e| DomainError::Repository {
                operation: "serialize decision to JSON",
                message: e.to_string(),
            })?;

        sqlx::query!(
            r#"
            INSERT INTO decisions (transaction_id, decision)
            VALUES ($1, $2)
            "#,
            transaction_id.as_uuid(),
            decision_json,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| DomainError::Repository {
            operation: "store decision",
            message: e.to_string(),
        })?;

        Ok(())
    }
}
