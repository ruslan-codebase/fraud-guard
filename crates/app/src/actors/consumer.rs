use crate::actors::messages::{
    AckMessage, EvaluateTransactionRequest, StartConsuming, StoreDelivery,
};
use async_trait::async_trait;
use fraud_guard_api::amqp::CorrelationId;
use fraud_guard_api::amqp::amqp_consumer::{AmqpConsumer, TransactionHandler};
use fraud_guard_domain::TransactionRequest;
use kameo::actor::Recipient;
use kameo::{
    Actor,
    actor::{ActorRef, WeakActorRef},
    error::ActorStopReason,
    message::{Context, Message},
};
use std::{collections::HashSet, sync::Arc};
use tracing::{error, info};

pub struct ConsumerActor {
    pub amqp_url: String,
    pub queue_name: String,
    pub actor_ref: Option<ActorRef<Self>>,
    pending: HashSet<CorrelationId>,
    amqp_consumer: Option<AmqpConsumer>,
}

struct Handler {
    weak_ref: WeakActorRef<ConsumerActor>,
    engine_ref: Recipient<EvaluateTransactionRequest>,
}

impl Handler {
    fn new(
        weak_ref: WeakActorRef<ConsumerActor>,
        engine_ref: Recipient<EvaluateTransactionRequest>,
    ) -> Self {
        Self {
            weak_ref,
            engine_ref,
        }
    }
}

#[async_trait]
impl TransactionHandler for Handler {
    async fn handle_transaction(
        &self,
        corr_id: CorrelationId,
        tx: TransactionRequest,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let Some(actor) = self.weak_ref.upgrade() {
            actor
                .tell(StoreDelivery {
                    correlation_id: corr_id,
                })
                .await
                .map_err(|e| format!("Actor not available: {}", e))?;
            self.engine_ref
                .tell(EvaluateTransactionRequest {
                    tx,
                    correlation_id: corr_id,
                })
                .await
                .map_err(|e| format!("Actor not available: {}", e))?;
            Ok(())
        } else {
            Err("ConsumerActor is gone".into())
        }
    }
}

impl ConsumerActor {
    pub fn new(amqp_url: String, queue_name: String) -> Self {
        Self {
            amqp_url,
            queue_name,
            actor_ref: None,
            pending: HashSet::new(),
            amqp_consumer: None,
        }
    }
}

impl Actor for ConsumerActor {
    type Args = Self;
    type Error = std::convert::Infallible;

    async fn on_start(
        mut state: Self::Args,
        actor_ref: ActorRef<Self>,
    ) -> Result<Self, Self::Error> {
        info!("ConsumerActor started");
        state.actor_ref = Some(actor_ref);
        Ok(state)
    }

    async fn on_stop(
        &mut self,
        _actor_ref: WeakActorRef<Self>,
        _reason: ActorStopReason,
    ) -> Result<(), Self::Error> {
        info!("ConsumerActor stopping gracefully");
        if let Some(mut consumer) = self.amqp_consumer.take() {
            consumer.stop();
        }
        Ok(())
    }
}

impl Message<StoreDelivery> for ConsumerActor {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: StoreDelivery,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let corr_id = msg.correlation_id;
        self.pending.insert(corr_id);
    }
}

impl Message<StartConsuming> for ConsumerActor {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: StartConsuming,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        if self.amqp_consumer.is_some() {
            return;
        }

        let weak_ref = self.actor_ref.as_ref().unwrap().downgrade();
        let handler = Arc::new(Handler::new(weak_ref, msg.engine_ref.clone()));

        let (consumer, ack_rx) =
            AmqpConsumer::new(self.amqp_url.clone(), self.queue_name.clone(), handler);

        self.amqp_consumer = Some(consumer);

        if let Some(consumer) = &mut self.amqp_consumer {
            consumer.start(ack_rx).await
        }
    }
}

impl Message<AckMessage> for ConsumerActor {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: AckMessage,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let corr_id = msg.correlation_id;
        let result = msg.result;

        if self.pending.remove(&corr_id) {
            match result {
                Ok(()) => {
                    if let Some(consumer) = &self.amqp_consumer {
                        consumer.ack(corr_id).await;
                    } else {
                        error!("AmqpConsumer not available for ack");
                    }
                }
                Err(e) => {
                    if let Some(consumer) = &self.amqp_consumer {
                        error!("Persistence failed for {}: {}, nacking", corr_id, e);
                        consumer.nack(corr_id).await;
                    } else {
                        error!("AmqpConsumer not available for nack");
                    }
                }
            }
        }
    }
}
