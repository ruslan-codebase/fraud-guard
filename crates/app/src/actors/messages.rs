use std::sync::Arc;

use fraud_guard_api::amqp::CorrelationId;
use fraud_guard_domain::{Decision, DomainError, Rule, TransactionRequest};
use kameo::actor::Recipient;

pub struct StoreDelivery {
    pub correlation_id: CorrelationId,
}

pub struct StartConsuming {
    pub engine_ref: Recipient<EvaluateTransactionRequest>,
}

pub struct AckMessage {
    pub correlation_id: CorrelationId,
    pub result: Result<(), DomainError>,
}

pub struct UpdateRules {
    pub rules: Arc<Vec<Rule>>,
}

pub struct EvaluateTransactionRequest {
    pub tx: TransactionRequest,
    pub correlation_id: CorrelationId,
}

pub struct PersistResults {
    pub tx: TransactionRequest,
    pub decision: Decision,
    pub correlation_id: CorrelationId,
}
