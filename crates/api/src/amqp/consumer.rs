use chrono::{DateTime, Utc};
use fraud_guard_domain::{AccountId, Currency, TransactionId, TransactionRequest};
use lapin::{
    Channel, Connection, ConnectionProperties,
    message::Delivery,
    options::{BasicAckOptions, BasicConsumeOptions, BasicNackOptions, QueueDeclareOptions},
    types::FieldTable,
};
use serde::Deserialize;
use std::error;
use std::str::FromStr;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tracing::{error, info};

#[derive(Debug, Deserialize)]
pub struct TransactionMessage {
    pub transaction_id: String,
    pub source_account: String,
    pub destination_account: String,
    pub amount: i64,
    pub currency: String,
    pub timestamp: Option<i64>,
}

pub struct ConsumerActor {
    pub amqp_url: String,
    pub queue_name: String,
    pub engine_tx: mpsc::Sender<TransactionRequest>,
}

impl ConsumerActor {
    pub async fn run(self) {
        loop {
            info!("Attempting to connect to AMQP...");
            match self.connect_and_consume().await {
                Ok(_) => {
                    info!("AMQP consumer diconnected, reconnecting in 5s...");
                }
                Err(e) => {
                    error!("AMQP consumer error: {}, reconnecting in 5s...", e);
                }
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }

    async fn connect_and_consume(&self) -> Result<(), Box<dyn error::Error + Send + Sync>> {
        let conn = Connection::connect(&self.amqp_url, ConnectionProperties::default()).await?;
        let channel = conn.create_channel().await?;

        channel
            .queue_declare(
                self.queue_name.as_str().into(),
                QueueDeclareOptions {
                    durable: true,
                    exclusive: false,
                    auto_delete: false,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await?;

        let mut consumer = channel
            .basic_consume(
                self.queue_name.as_str().into(),
                "fraud_guard_consumer".into(),
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await?;

        info!("AMQP consumer started on queue {}", self.queue_name);

        while let Some(delivery) = consumer.next().await {
            match delivery {
                Ok(delivery) => {
                    let delivery_tag = delivery.delivery_tag;
                    if let Err(e) = self.handle_delivery(&channel, delivery).await {
                        error!("Failed to process message: {}", e);
                        let _ = channel
                            .basic_nack(delivery_tag, BasicNackOptions::default())
                            .await;
                    }
                }
                Err(e) => {
                    error!("Consumer error: {}", e);
                    return Err(e.into());
                }
            }
        }

        Ok(())
    }

    async fn handle_delivery(
        &self,
        channel: &Channel,
        delivery: Delivery,
    ) -> Result<(), Box<dyn error::Error + Send + Sync>> {
        let payload = String::from_utf8_lossy(&delivery.data);
        let msg: TransactionMessage = serde_json::from_str(&payload)?;

        let source = AccountId::new(&msg.source_account)?;
        let dest = AccountId::new(&msg.destination_account)?;
        let currency = Currency::from_str(&msg.currency)?;
        let timestamp = msg
            .timestamp
            .and_then(|ts| DateTime::from_timestamp(ts, 0))
            .unwrap_or_else(Utc::now);

        let tx = TransactionRequest {
            id: TransactionId::from_uuid(uuid::Uuid::parse_str(&msg.transaction_id)?),
            source_account: source,
            destination_account: dest,
            amount: msg.amount,
            currency,
            timestamp,
        };

        self.engine_tx.send(tx).await?;

        channel
            .basic_ack(delivery.delivery_tag, BasicAckOptions::default())
            .await?;

        info!("Processed transaction {}", msg.transaction_id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fraud_guard_domain::DomainError;
    use serde_json::json;

    #[test]
    fn transaction_message_deserialization() {
        let json = json!({
            "transaction_id": "550e8400-e29b-41d4-a716-446655440000",
            "source_account": "1111222233334444",
            "destination_account": "5555666677778888",
            "amount": 10000,
            "currency": "USD",
            "timestamp": 1609459200
        });
        let msg: TransactionMessage = serde_json::from_value(json).unwrap();
        assert_eq!(msg.transaction_id, "550e8400-e29b-41d4-a716-446655440000");
        assert_eq!(msg.source_account, "1111222233334444");
        assert_eq!(msg.destination_account, "5555666677778888");
        assert_eq!(msg.amount, 10000);
        assert_eq!(msg.currency, "USD");
        assert_eq!(msg.timestamp, Some(1609459200));
    }

    #[test]
    fn transaction_message_to_transaction_request() {
        let json = json!({
            "transaction_id": "550e8400-e29b-41d4-a716-446655440000",
            "source_account": "1111222233334444",
            "destination_account": "5555666677778888",
            "amount": 10000,
            "currency": "USD",
            "timestamp": 1609459200
        });
        let msg: TransactionMessage = serde_json::from_value(json).unwrap();
        let source = AccountId::new(&msg.source_account).unwrap();
        let dest = AccountId::new(&msg.destination_account).unwrap();
        let currency = Currency::from_str(&msg.currency).unwrap();
        let timestamp = chrono::DateTime::from_timestamp(msg.timestamp.unwrap(), 0).unwrap();

        let tx = TransactionRequest {
            id: TransactionId::from_uuid(uuid::Uuid::parse_str(&msg.transaction_id).unwrap()),
            source_account: source,
            destination_account: dest,
            amount: msg.amount,
            currency,
            timestamp,
        };

        assert_eq!(tx.amount, 10000);
        assert_eq!(tx.source_account.as_str(), "1111222233334444");
        assert_eq!(tx.destination_account.as_str(), "5555666677778888");
        assert_eq!(tx.currency, Currency::Usd);
    }

    #[test]
    fn invalid_currency_parsing() {
        let json = json!({
            "transaction_id": "550e8400-e29b-41d4-a716-446655440000",
            "source_account": "1111222233334444",
            "destination_account": "5555666677778888",
            "amount": 10000,
            "currency": "XYZ",
            "timestamp": 1609459200
        });
        let msg: TransactionMessage = serde_json::from_value(json).unwrap();
        let err = Currency::from_str(&msg.currency).unwrap_err();
        match err {
            DomainError::Validation(msg) => assert_eq!(msg, "Invalid currency: XYZ"),
            _ => panic!("Expected Validation error"),
        }
    }

    #[test]
    fn invalid_account_id_parsing() {
        let json = json!({
            "transaction_id": "550e8400-e29b-41d4-a716-446655440000",
            "source_account": "123", // too short
            "destination_account": "5555666677778888",
            "amount": 10000,
            "currency": "USD",
            "timestamp": 1609459200
        });
        let msg: TransactionMessage = serde_json::from_value(json).unwrap();
        let err = AccountId::new(&msg.source_account).unwrap_err();
        match err {
            DomainError::Validation(msg) => {
                assert_eq!(msg, "Account ID must be exactly 16 characters long")
            }
            _ => panic!("Expected Validation error"),
        }
    }
}
