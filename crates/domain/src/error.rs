use thiserror::Error;

#[derive(Error, Debug)]
pub enum DomainError {
    #[error("Validation error: {0}")]
    Validation(String),

    #[error("Parameter parsing error for rule {rule_code}: {source}")]
    ParamParse {
        rule_code: String,
        source: serde_json::Error,
    },
}
