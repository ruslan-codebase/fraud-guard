use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use fraud_guard_domain::{AccountId, Currency, TransactionId, TransactionRequest};
use lapin::{
    Connection, ConnectionProperties, ExchangeKind,
    message::Delivery,
    options::{
        BasicAckOptions, BasicConsumeOptions, BasicNackOptions, ExchangeDeclareOptions,
        QueueBindOptions, QueueDeclareOptions,
    },
    types::{FieldTable, ShortString},
};
use serde::Deserialize;
use std::str::FromStr;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tracing::{error, info};

use crate::amqp::CorrelationId;

const DLX_EXCHANGE: &str = "dlx.transaction";
const DLQ_QUEUE: &str = "transaction.evaluation.dlq";

#[derive(Debug, Deserialize)]
pub struct TransactionMessage {
    pub transaction_id: String,
    pub source_account: String,
    pub destination_account: String,
    pub amount: i64,
    pub currency: String,
    pub timestamp: Option<i64>,
}

pub enum AckCommand {
    Ack(CorrelationId),
    Nack(CorrelationId),
}

#[async_trait]
pub trait TransactionHandler: Send + Sync {
    async fn handle_transaction(
        &self,
        corr_id: CorrelationId,
        tx: TransactionRequest,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

pub struct AmqpConsumer {
    amqp_url: String,
    queue_name: String,
    command_tx: mpsc::Sender<AckCommand>,
    handler: Arc<dyn TransactionHandler>,
    task_handle: Option<tokio::task::JoinHandle<()>>,
}

impl AmqpConsumer {
    pub fn new(
        amqp_url: String,
        queue_name: String,
        handler: Arc<dyn TransactionHandler>,
    ) -> (Self, mpsc::Receiver<AckCommand>) {
        let (tx, rx) = mpsc::channel(100);
        (
            Self {
                amqp_url,
                queue_name,
                command_tx: tx,
                task_handle: None,
                handler,
            },
            rx,
        )
    }

    pub async fn start(&mut self, mut command_rx: mpsc::Receiver<AckCommand>) {
        if self.task_handle.is_some() {
            return;
        }

        let amqp_url = self.amqp_url.clone();
        let queue_name = self.queue_name.clone();
        let handler = self.handler.clone();
        let handle = tokio::spawn(async move {
            Self::run_consumer(amqp_url, queue_name, handler, &mut command_rx).await;
        });
        self.task_handle = Some(handle);
    }

    pub fn stop(&mut self) {
        if let Some(handle) = self.task_handle.take() {
            handle.abort();
        }
    }

    pub async fn ack(&self, corr_id: CorrelationId) {
        let _ = self.command_tx.send(AckCommand::Ack(corr_id)).await;
    }

    pub async fn nack(&self, corr_id: CorrelationId) {
        let _ = self.command_tx.send(AckCommand::Nack(corr_id)).await;
    }

    async fn run_consumer(
        amqp_url: String,
        queue_name: String,
        handler: Arc<dyn TransactionHandler>,
        command_rx: &mut mpsc::Receiver<AckCommand>,
    ) {
        let mut backoff = 1;
        loop {
            match Self::connect_and_consume(&amqp_url, &queue_name, handler.clone(), command_rx)
                .await
            {
                Ok(()) => {
                    info!("Consumer disconnected, reconnecting in 5s...");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    backoff = 1;
                }
                Err(e) => {
                    error!("Consumer error: {}, reconnecting in {}s", e, backoff);
                    tokio::time::sleep(Duration::from_secs(backoff)).await;
                    backoff = std::cmp::min(backoff * 2, 60);
                }
            }
        }
    }

    async fn connect_and_consume(
        amqp_url: &str,
        queue_name: &str,
        handler: Arc<dyn TransactionHandler>,
        command_rx: &mut mpsc::Receiver<AckCommand>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let conn = Connection::connect(amqp_url, ConnectionProperties::default()).await?;
        let channel = conn.create_channel().await?;

        channel
            .exchange_declare(
                DLX_EXCHANGE.into(),
                ExchangeKind::Direct,
                ExchangeDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await?;

        channel
            .queue_declare(
                DLQ_QUEUE.into(),
                QueueDeclareOptions {
                    durable: true,
                    exclusive: false,
                    auto_delete: false,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await?;

        channel
            .queue_bind(
                DLQ_QUEUE.into(),
                DLX_EXCHANGE.into(),
                DLQ_QUEUE.into(),
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await?;

        let mut args = FieldTable::default();
        args.insert(
            "x-dead-letter-exchange".into(),
            ShortString::from(DLX_EXCHANGE).into(),
        );
        args.insert(
            "x-dead-letter-routing-key".into(),
            ShortString::from(DLQ_QUEUE).into(),
        );

        channel
            .queue_declare(
                queue_name.into(),
                QueueDeclareOptions {
                    durable: true,
                    exclusive: false,
                    auto_delete: false,
                    ..Default::default()
                },
                args,
            )
            .await?;

        let mut consumer = channel
            .basic_consume(
                queue_name.into(),
                "fraud_guard_consumer".into(),
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await?;

        loop {
            tokio::select! {
                Some(delivery) = consumer.next() => {
                    let delivery = match delivery {
                        Ok(d) => d,
                        Err(e) => {
                            error!("Delivery error: {}", e);
                            return Err(e.into());
                        }
                    };
                    let corr_id = delivery.delivery_tag;
                    let tx = match Self::parse_delivery(&delivery) {
                        Ok(tx) => tx,
                        Err(e) => {
                            error!("Failed to parse delivery: {}", e);
                            let _ = channel.basic_nack(corr_id, BasicNackOptions::default()).await;
                            continue;
                        }
                    };
                    match handler.handle_transaction(corr_id, tx).await {
                        Ok(()) => {

                        }
                        Err(e) => {
                            error!("Callback error: {}, nacking to DLQ", e);
                            let _ = channel.basic_nack(corr_id, BasicNackOptions::default()).await;
                        }
                    }
                }
                Some(cmd) = command_rx.recv() => {
                    match cmd {
                        AckCommand::Ack(tag) => {
                            let _ = channel.basic_ack(tag, BasicAckOptions::default()).await;
                        }
                        AckCommand::Nack(tag) => {
                            let _ = channel.basic_nack(tag, BasicNackOptions::default()).await;
                        }
                    }
                }
                else => break,
            }
        }
        Ok(())
    }

    fn parse_delivery(
        delivery: &Delivery,
    ) -> Result<TransactionRequest, Box<dyn std::error::Error + Send + Sync>> {
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
