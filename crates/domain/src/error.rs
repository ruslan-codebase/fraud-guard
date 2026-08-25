use thiserror::Error;

#[derive(Error, Debug)]
pub enum DomainError {
    #[error("Validation error: {0}")]
    Validation(String),

    #[error("Parameter parsing error for rule {rule_code}: {message}")]
    ParamParse { rule_code: String, message: String },

    #[error("Repository error while {operation}: {message}")]
    Repository {
        operation: &'static str,
        message: String,
    },
}
