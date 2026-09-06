use serde::Deserialize;
use std::env;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("Configuration error: {0}")]
    Generic(String),
}

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub database_url: String,
    pub amqp_url: String,
    pub log_level: String,
    pub rules_refresh_interval_secs: u64,
}

impl Config {
    pub fn from_env() -> Result<Self, ConfigError> {
        let _ = dotenvy::dotenv();

        let database_url = env::var("DATABASE_URL")
            .map_err(|e| ConfigError::Generic(format!("DATABASE_URL: {}", e)))?;

        let amqp_url =
            env::var("AMQP_URL").map_err(|e| ConfigError::Generic(format!("AMQP_URL: {}", e)))?;

        let log_level =
            env::var("RUST_LOG").map_err(|e| ConfigError::Generic(format!("RUST_LOG: {}", e)))?;

        let rules_refresh_interval_secs = env::var("RULES_REFRESH_INTERVAL_SECS")
            .ok()
            .map(|s| {
                s.parse().map_err(|e| {
                    ConfigError::Generic(format!("RULES_REFRESH_INTERVAL_SECS: {}", e))
                })
            })
            .transpose()?
            .unwrap_or(60);

        Ok(Config {
            database_url,
            amqp_url,
            log_level,
            rules_refresh_interval_secs,
        })
    }
}
