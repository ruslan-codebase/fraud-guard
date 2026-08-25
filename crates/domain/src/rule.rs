use crate::{DomainError, TransactionRepository, TransactionRequest};
use chrono::{DateTime, Utc};
use serde::Deserialize;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuleType {
    Threshold,
    Blacklist,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rule {
    pub code: String,
    pub rule_type: RuleType,
    pub params: serde_json::Value,
    pub enabled: bool,
    pub priority: i32,
    pub valid_from: DateTime<Utc>,
    pub valid_until: Option<DateTime<Utc>>,
}

#[derive(Debug, Deserialize)]
struct AmountLimitParams {
    max_amount: i64,
}

#[derive(Debug, Deserialize)]
struct BlacklistParams {
    blacklisted_accounts: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    Passed,
    Skipped { reason: String },
    Triggered { code: String, reason: String },
}

impl Rule {
    pub fn is_active_at(&self, at: DateTime<Utc>) -> bool {
        if !self.enabled {
            return false;
        }
        if at < self.valid_from {
            return false;
        }
        if let Some(until) = self.valid_until
            && at >= until
        {
            return false;
        }
        true
    }

    pub fn evaluate(
        &self,
        tx_req: &TransactionRequest,
        _repo: &impl TransactionRepository,
    ) -> Result<Verdict, DomainError> {
        if !self.is_active_at(tx_req.timestamp) {
            return Ok(Verdict::Skipped {
                reason: "Rule is not active at this time".to_string(),
            });
        }

        match self.rule_type {
            RuleType::Threshold => {
                let params: AmountLimitParams = serde_json::from_value(self.params.clone())
                    .map_err(|e| DomainError::ParamParse {
                        rule_code: self.code.clone(),
                        source: e,
                    })?;

                if tx_req.amount > params.max_amount {
                    Ok(Verdict::Triggered {
                        code: self.code.clone(),
                        reason: format!(
                            "Amount {} exceeds limit {}",
                            tx_req.amount, params.max_amount
                        ),
                    })
                } else {
                    Ok(Verdict::Passed)
                }
            }
            RuleType::Blacklist => {
                let params: BlacklistParams =
                    serde_json::from_value(self.params.clone()).map_err(|e| {
                        DomainError::ParamParse {
                            rule_code: self.code.clone(),
                            source: e,
                        }
                    })?;

                let source = tx_req.source_account.as_str();
                if params.blacklisted_accounts.contains(&source.to_string()) {
                    Ok(Verdict::Triggered {
                        code: self.code.clone(),
                        reason: format!("Account {} is blacklisted", source),
                    })
                } else {
                    Ok(Verdict::Passed)
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AccountId, Currency, TransactionId};
    use async_trait::async_trait;
    use chrono::Duration;

    struct MockRepo;

    #[async_trait]
    impl TransactionRepository for MockRepo {
        async fn get_transactions_for_account(
            &self,
            _account: &AccountId,
            _from: DateTime<Utc>,
            _to: DateTime<Utc>,
        ) -> Result<Vec<TransactionRequest>, DomainError> {
            Ok(vec![])
        }
    }

    fn test_transaction(amount: i64) -> TransactionRequest {
        let source = AccountId::new("1111222233334444").unwrap();
        let dest = AccountId::new("5555666677778888").unwrap();
        TransactionRequest {
            id: TransactionId::new(),
            source_account: source,
            destination_account: dest,
            amount,
            currency: Currency::Rub,
            timestamp: Utc::now(),
        }
    }

    fn active_rule(rule_type: RuleType, params: serde_json::Value) -> Rule {
        Rule {
            code: "TEST001".to_string(),
            rule_type,
            params,
            enabled: true,
            priority: 10,
            valid_from: DateTime::parse_from_rfc3339("2020-01-01T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            valid_until: None,
        }
    }

    fn inactive_rule(rule_type: RuleType, params: serde_json::Value) -> Rule {
        let mut rule = active_rule(rule_type, params);
        rule.enabled = false;
        rule
    }

    #[test]
    fn threshold_passed() {
        let rule = active_rule(RuleType::Threshold, serde_json::json!({"max_amount": 1000}));
        let tx_req = test_transaction(500);
        let verdict = rule.evaluate(&tx_req, &MockRepo).unwrap();
        assert_eq!(verdict, Verdict::Passed);
    }

    #[test]
    fn threshold_triggered() {
        let rule = active_rule(RuleType::Threshold, serde_json::json!({"max_amount": 1000}));
        let tx_req = test_transaction(1500);
        let verdict = rule.evaluate(&tx_req, &MockRepo).unwrap();
        match verdict {
            Verdict::Triggered { code, reason } => {
                assert_eq!(code, "TEST001");
                assert!(reason.contains("1500 exceeds limit 1000"));
            }
            _ => panic!("Expected Triggered, got {:?}", verdict),
        }
    }

    #[test]
    fn threshold_invalid_params() {
        let rule = active_rule(
            RuleType::Threshold,
            serde_json::json!({"wrong_field": 1000}),
        );
        let tx_req = test_transaction(500);
        let err = rule.evaluate(&tx_req, &MockRepo).unwrap_err();
        match err {
            DomainError::ParamParse {
                rule_code,
                source: _,
            } => {
                assert_eq!(rule_code, "TEST001");
            }
            _ => panic!("Expected ParamParse error, got {:?}", err),
        }
    }

    #[test]
    fn blacklist_triggered() {
        let rule = active_rule(
            RuleType::Blacklist,
            serde_json::json!({"blacklisted_accounts": ["1111222233334444"]}),
        );
        let tx_req = test_transaction(100);
        let verdict = rule.evaluate(&tx_req, &MockRepo).unwrap();
        match verdict {
            Verdict::Triggered { code, reason } => {
                assert_eq!(code, "TEST001");
                assert!(reason.contains("1111222233334444 is blacklisted"));
            }
            _ => panic!("Expected Triggered, got {:?}", verdict),
        }
    }

    #[test]
    fn blacklist_invalid_params() {
        let rule = active_rule(
            RuleType::Blacklist,
            serde_json::json!({"wrong_field": ["1111"]}),
        );
        let tx_req = test_transaction(100);
        let err = rule.evaluate(&tx_req, &MockRepo).unwrap_err();
        match err {
            DomainError::ParamParse {
                rule_code,
                source: _,
            } => {
                assert_eq!(rule_code, "TEST001");
            }
            _ => panic!("Expected ParamParse error, got {:?}", err),
        }
    }

    #[test]
    fn inactive_rule_skipped() {
        let rule = inactive_rule(RuleType::Threshold, serde_json::json!({"max_amount": 1000}));
        let tx_req = test_transaction(500);
        let verdict = rule.evaluate(&tx_req, &MockRepo).unwrap();
        match verdict {
            Verdict::Skipped { reason } => {
                assert_eq!(reason, "Rule is not active at this time");
            }
            _ => panic!("Expected Skipped, got {:?}", verdict),
        }
    }

    #[test]
    fn rule_not_yet_valid_skipped() {
        let mut rule = active_rule(RuleType::Threshold, serde_json::json!({"max_amount": 1000}));
        rule.valid_from = Utc::now() + Duration::days(1);
        let tx_req = test_transaction(500);
        let verdict = rule.evaluate(&tx_req, &MockRepo).unwrap();
        match verdict {
            Verdict::Skipped { reason } => {
                assert_eq!(reason, "Rule is not active at this time");
            }
            _ => panic!("Expected Skipped, got {:?}", verdict),
        }
    }

    #[test]
    fn rule_expired_skipped() {
        let mut rule = active_rule(RuleType::Threshold, serde_json::json!({"max_amount": 1000}));
        rule.valid_until = Some(Utc::now() - Duration::days(1));
        let tx_req = test_transaction(500);
        let verdict = rule.evaluate(&tx_req, &MockRepo).unwrap();
        match verdict {
            Verdict::Skipped { reason } => {
                assert_eq!(reason, "Rule is not active at this time");
            }
            _ => panic!("Expected Skipped, got {:?}", verdict),
        }
    }

    #[test]
    fn rule_is_active_at() {
        let rule = Rule {
            code: "THR001".to_string(),
            rule_type: RuleType::Threshold,
            params: serde_json::json!({"max_amount": 100000}),
            enabled: true,
            priority: 10,
            valid_from: DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            valid_until: Some(
                DateTime::parse_from_rfc3339("2026-12-31T23:59:59Z")
                    .unwrap()
                    .with_timezone(&Utc),
            ),
        };

        let mid = DateTime::parse_from_rfc3339("2026-06-15T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        assert!(rule.is_active_at(mid));

        let before = DateTime::parse_from_rfc3339("2025-12-31T23:59:59Z")
            .unwrap()
            .with_timezone(&Utc);
        assert!(!rule.is_active_at(before));

        let after = DateTime::parse_from_rfc3339("2027-01-01T00:00:01Z")
            .unwrap()
            .with_timezone(&Utc);
        assert!(!rule.is_active_at(after));
    }

    #[test]
    fn rule_is_active_at_when_disabled() {
        let rule = Rule {
            code: "THR001".to_string(),
            rule_type: RuleType::Threshold,
            params: serde_json::json!({"max_amount": 100000}),
            enabled: false,
            priority: 10,
            valid_from: DateTime::parse_from_rfc3339("2025-01-01T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            valid_until: None,
        };

        let now = Utc::now();
        assert!(!rule.is_active_at(now));
    }

    #[test]
    fn rule_is_active_with_no_expiry() {
        let rule = Rule {
            code: "THR001".to_string(),
            rule_type: RuleType::Threshold,
            params: serde_json::json!({"max_amount": 100000}),
            enabled: true,
            priority: 10,
            valid_from: DateTime::parse_from_rfc3339("2020-01-01T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            valid_until: None,
        };

        let far_future = DateTime::parse_from_rfc3339("2099-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        assert!(rule.is_active_at(far_future));
    }
}
