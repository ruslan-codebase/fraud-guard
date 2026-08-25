use crate::DomainError;
use chrono::{DateTime, Utc};
use std::fmt;
use std::str::FromStr;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TransactionId(Uuid);

impl TransactionId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    pub fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }

    pub fn as_uuid(&self) -> Uuid {
        self.0
    }
}

impl std::fmt::Display for TransactionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountId(String);

impl AccountId {
    pub fn new(s: &str) -> Result<Self, DomainError> {
        let trimmed = s.trim();
        if trimmed.len() != 16 {
            return Err(DomainError::Validation(
                "Account ID must be exactly 16 characters long".to_string(),
            ));
        }
        if !trimmed.chars().all(|c| c.is_ascii_digit()) {
            return Err(DomainError::Validation(
                "Account ID must contain only digits".to_string(),
            ));
        }
        Ok(AccountId(trimmed.to_string()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for AccountId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for AccountId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for AccountId {
    type Err = DomainError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        AccountId::new(s)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Currency {
    Rub,
    Cny,
    Usd,
    Eur,
}

impl fmt::Display for Currency {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Currency::Rub => "RUB",
            Currency::Cny => "CNY",
            Currency::Usd => "USD",
            Currency::Eur => "EUR",
        };
        write!(f, "{}", s)
    }
}

impl FromStr for Currency {
    type Err = DomainError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "RUB" => Ok(Currency::Rub),
            "CNY" => Ok(Currency::Cny),
            "USD" => Ok(Currency::Usd),
            "EUR" => Ok(Currency::Eur),
            _ => Err(DomainError::Validation(format!("Invalid currency: {}", s))),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TransactionRequest {
    pub id: TransactionId,
    pub source_account: AccountId,
    pub destination_account: AccountId,
    pub amount: i64, // amount in smallest unit (kopeiki, cents)
    pub currency: Currency,
    pub timestamp: DateTime<Utc>,
}

impl TransactionRequest {
    pub fn new(source: AccountId, destination: AccountId, amount: i64, currency: Currency) -> Self {
        Self {
            id: TransactionId::new(),
            source_account: source,
            destination_account: destination,
            amount,
            currency,
            timestamp: Utc::now(),
        }
    }

    pub fn validate(&self) -> Result<(), DomainError> {
        if self.amount <= 0 {
            return Err(DomainError::Validation(
                "Amount must be positive".to_string(),
            ));
        }
        if self.source_account == self.destination_account {
            return Err(DomainError::Validation(
                "Source and destination accounts must differ".to_string(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn transaction_id_creation() {
        let id1 = TransactionId::new();
        let id2 = TransactionId::new();
        assert_ne!(id1, id2);

        let uuid = Uuid::new_v4();
        let id3 = TransactionId::from_uuid(uuid);
        assert_eq!(id3.as_uuid(), uuid);
    }

    #[test]
    fn account_id_valid() {
        let account = AccountId::new("1234567890123456").unwrap();
        assert_eq!(account.as_str(), "1234567890123456");
        assert_eq!(account.to_string(), "1234567890123456");
    }

    #[test]
    fn account_id_invalid_length() {
        let err = AccountId::new("123").unwrap_err();
        assert!(
            matches!(err, DomainError::Validation(msg) if msg == "Account ID must be exactly 16 characters long")
        );
    }

    #[test]
    fn account_id_invalid_characters() {
        let err = AccountId::new("1234567890abcdef").unwrap_err();
        assert!(
            matches!(err, DomainError::Validation(msg) if msg == "Account ID must contain only digits")
        );
    }

    #[test]
    fn account_id_from_str() {
        let account = AccountId::from_str("1111222233334444").unwrap();
        assert_eq!(account.as_str(), "1111222233334444");
    }

    #[test]
    fn transaction_request_validate_ok() {
        let source = AccountId::new("1111222233334444").unwrap();
        let dest = AccountId::new("5555666677778888").unwrap();
        let tx_req = TransactionRequest::new(source, dest, 10000, Currency::Rub);
        assert!(tx_req.validate().is_ok())
    }

    #[test]
    fn transaction_request_validate_zero_amount() {
        let source = AccountId::new("1111222233334444").unwrap();
        let dest = AccountId::new("5555666677778888").unwrap();
        let tx_req = TransactionRequest::new(source, dest, 0, Currency::Rub);
        let err = tx_req.validate().unwrap_err();
        assert!(matches!(err, DomainError::Validation(msg) if msg == "Amount must be positive"))
    }

    #[test]
    fn transaction_request_validate_negative_amount() {
        let source = AccountId::new("1111222233334444").unwrap();
        let dest = AccountId::new("5555666677778888").unwrap();
        let tx_req = TransactionRequest::new(source, dest, -100, Currency::Rub);
        let err = tx_req.validate().unwrap_err();
        assert!(matches!(err, DomainError::Validation(msg) if msg == "Amount must be positive"))
    }

    #[test]
    fn transaction_request_validate_same_account() {
        let account = AccountId::new("1111222233334444").unwrap();
        let tx_req = TransactionRequest::new(account.clone(), account, 10000, Currency::Rub);
        let err = tx_req.validate().unwrap_err();
        assert!(
            matches!(err, DomainError::Validation(msg) if msg == "Source and destination accounts must differ")
        );
    }
}
