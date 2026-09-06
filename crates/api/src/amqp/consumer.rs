use super::CorrelationId;
use chrono::{DateTime, Utc};
use fraud_guard_domain::{AccountId, Currency, DomainError, TransactionId, TransactionRequest};
use lapin::{
    Connection, ConnectionProperties,
    message::Delivery,
    options::{BasicAckOptions, BasicConsumeOptions, BasicNackOptions, QueueDeclareOptions},
    types::FieldTable,
};
use serde::Deserialize;
use std::str::FromStr;
use std::time::Duration;
use std::{collections::HashMap, error};
use tokio::sync::{mpsc, watch};
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
    pub engine_tx: mpsc::Sender<(TransactionRequest, CorrelationId)>,
    pub ack_rx: mpsc::Receiver<(CorrelationId, Result<(), DomainError>)>,
    pub shutdown_rx: watch::Receiver<()>,
}

enum ConnectionResult {
    Shutdown,
    Disconnected,
}

impl ConsumerActor {
    pub async fn run(mut self) {
        let mut backoff = 1;
        let mut shutdown_rx_clone = self.shutdown_rx.clone();

        loop {
            tokio::select! {
                _ = shutdown_rx_clone.changed() => {
                    info!("Shutdown signal received, exiting consumer");
                    break;
                }
                result = self.connect_and_consume() => {
                    match result {
                        Ok(ConnectionResult::Shutdown) => {
                            info!("Shutdown signal received, exiting consumer");
                            break;
                        }
                        Ok(ConnectionResult::Disconnected) => {
                            info!("AMQP consumer disconnected, reconnecting in 5s...");
                            tokio::time::sleep(Duration::from_secs(5)).await;
                            backoff = 1;
                        }
                        Err(e) => {
                            error!("AMQP consumer error: {}, reconnecting in {}s", e, backoff);
                            tokio::time::sleep(Duration::from_secs(backoff)).await;
                            backoff = std::cmp::min(backoff * 2, 60);
                        }
                    }
                }
            }
        }
    }

    async fn connect_and_consume(
        &mut self,
    ) -> Result<ConnectionResult, Box<dyn error::Error + Send + Sync>> {
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

        let mut pending: HashMap<CorrelationId, Delivery> = HashMap::new();

        loop {
            tokio::select! {
                _ = self.shutdown_rx.changed() => {
                    return Ok(ConnectionResult::Shutdown);
                }
                Some(delivery) = consumer.next() => {
                    let delivery = match delivery {
                        Ok(d) => d,
                        Err(e) => {
                            error!("Consumer delivery error: {}", e);
                            return Err(e.into());
                        }
                    };
                    let corr_id = delivery.delivery_tag;
                    let tx = match self.parse_delivery(&delivery) {
                        Ok(tx) => tx,
                        Err(e) => {
                            error!("Failed to parse delivery: {}", e);
                            let _ = channel.basic_nack(corr_id, BasicNackOptions::default()).await;
                            continue;
                        }
                    };
                    if let Err(e) = self.engine_tx.send((tx, corr_id)).await {
                        error!("Engine channel closed: {}", e);
                        let _ = channel.basic_nack(corr_id, BasicNackOptions::default()).await;
                        continue;
                    }
                    pending.insert(corr_id, delivery);
                    info!("Transaction {} sent to engine", corr_id);
                }
                Some((corr_id, result)) = self.ack_rx.recv() => {
                    if let Some(delivery) = pending.remove(&corr_id) {
                        match result {
                            Ok(()) => {
                                if let Err(e) = channel.basic_ack(delivery.delivery_tag, BasicAckOptions::default()).await {
                                    error!("Failed to ack message {}: {}", corr_id, e);
                                } else {
                                    info!("Acked message {}", corr_id);
                                }
                            }
                            Err(e) => {
                                error!("Processing failed for {}: {}, nacking", corr_id, e);
                                if let Err(e) = channel.basic_nack(delivery.delivery_tag, BasicNackOptions::default()).await {
                                    error!("Failed to nack message {}: {}", corr_id, e);
                                }
                            }
                        }
                    } else {
                        error!("Received ack for unknown correlation_id {}", corr_id);
                    }
                }
                else => break,
            }
        }

        Ok(ConnectionResult::Disconnected)
    }

    fn parse_delivery(
        &self,
        delivery: &Delivery,
    ) -> Result<TransactionRequest, Box<dyn error::Error + Send + Sync>> {
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
        Ok(tx)
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

    #[test]
    fn parse_delivery_success() {
        let (_, dummy_rx) = watch::channel(());
        let actor = ConsumerActor {
            amqp_url: "".to_string(),
            queue_name: "".to_string(),
            engine_tx: mpsc::channel(1).0,
            ack_rx: mpsc::channel(1).1,
            shutdown_rx: dummy_rx,
        };

        let payload = r#"{
            "transaction_id": "550e8400-e29b-41d4-a716-446655440000",
            "source_account": "1111222233334444",
            "destination_account": "5555666677778888",
            "amount": 10000,
            "currency": "RUB",
            "timestamp": 1609459200
        }"#;

        let delivery = Delivery::mock(42, "".into(), "".into(), false, payload.as_bytes().to_vec());

        let tx = actor.parse_delivery(&delivery).unwrap();
        assert_eq!(tx.amount, 10000);
        assert_eq!(tx.source_account.as_str(), "1111222233334444");
        assert_eq!(tx.destination_account.as_str(), "5555666677778888");
        assert_eq!(tx.currency, Currency::Rub)
    }

    #[test]
    fn parse_delivery_invalid_currency() {
        let (_, dummy_rx) = watch::channel(());
        let actor = ConsumerActor {
            amqp_url: "".to_string(),
            queue_name: "".to_string(),
            engine_tx: mpsc::channel(1).0,
            ack_rx: mpsc::channel(1).1,
            shutdown_rx: dummy_rx,
        };

        let payload = r#"{
            "transaction_id": "550e8400-e29b-41d4-a716-446655440000",
            "source_account": "1111222233334444",
            "destination_account": "5555666677778888",
            "amount": 10000,
            "currency": "XYZ",
            "timestamp": 1609459200
        }"#;

        let delivery = Delivery::mock(42, "".into(), "".into(), false, payload.as_bytes().to_vec());

        let err = actor.parse_delivery(&delivery).unwrap_err();
        assert!(err.to_string().contains("Invalid currency"));
    }

    #[test]
    fn parse_delivery_invalid_json() {
        let (_, dummy_rx) = watch::channel(());
        let actor = ConsumerActor {
            amqp_url: "".to_string(),
            queue_name: "".to_string(),
            engine_tx: mpsc::channel(1).0,
            ack_rx: mpsc::channel(1).1,
            shutdown_rx: dummy_rx,
        };

        let payload = "invalid json payload";
        let delivery = Delivery::mock(42, "".into(), "".into(), false, payload.as_bytes().to_vec());

        let err = actor.parse_delivery(&delivery).unwrap_err();
        assert!(err.to_string().contains("expected"));
    }
}
