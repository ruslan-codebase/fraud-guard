use async_trait::async_trait;
use chrono::Utc;
use fraud_guard_domain::{
    AccountId, Currency, DomainError, TransactionId, TransactionRepository, TransactionRequest,
};
use sqlx::PgPool;

pub struct PostgresTransactionRepository {
    pool: PgPool,
}

impl PostgresTransactionRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl TransactionRepository for PostgresTransactionRepository {
    async fn get_transactions_for_account(
        &self,
        account: &AccountId,
        from: chrono::DateTime<Utc>,
        to: chrono::DateTime<Utc>,
    ) -> Result<Vec<TransactionRequest>, DomainError> {
        let rows = sqlx::query!(
            r#"
            SELECT
                transaction_id,
                source_account,
                destination_account,
                amount,
                currency,
                timestamp
            FROM transactions
            WHERE source_account = $1
              AND timestamp >= $2
              AND timestamp < $3
            ORDER BY timestamp ASC
            "#,
            account.as_str(),
            from,
            to,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| DomainError::Repository {
            operation: "fetch transactions from DB",
            message: e.to_string(),
        })?;

        let mut requests = Vec::with_capacity(rows.len());
        for row in rows {
            let source =
                AccountId::new(&row.source_account).map_err(|e| DomainError::Repository {
                    operation: "parse source account from DB",
                    message: e.to_string(),
                })?;
            let dest =
                AccountId::new(&row.destination_account).map_err(|e| DomainError::Repository {
                    operation: "parse destination account from DB",
                    message: e.to_string(),
                })?;
            let currency: Currency =
                row.currency
                    .parse()
                    .map_err(|e: DomainError| DomainError::Repository {
                        operation: "parse currency from DB",
                        message: e.to_string(),
                    })?;

            requests.push(TransactionRequest {
                id: TransactionId::from_uuid(row.transaction_id),
                source_account: source,
                destination_account: dest,
                amount: row.amount,
                currency,
                timestamp: row.timestamp,
            });
        }
        Ok(requests)
    }

    async fn insert_transaction(&self, tx_req: &TransactionRequest) -> Result<(), DomainError> {
        sqlx::query!(
            r#"
            INSERT INTO transactions (transaction_id, source_account, destination_account, amount, currency, timestamp)
            VALUES ($1, $2, $3, $4, $5, $6)
            "#,
            tx_req.id.as_uuid(),
            tx_req.source_account.as_str(),
            tx_req.destination_account.as_str(),
            tx_req.amount,
            tx_req.currency.to_string(),
            tx_req.timestamp,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| DomainError::Repository {
            operation: "insert transaction into DB",
            message:  e.to_string()
        })?;
        Ok(())
    }
}
