use chrono::{DateTime, Utc};

pub enum RuleType {
    Threshold,
    Blacklist,
}

pub struct Rule {
    pub code: String,
    pub rule_type: RuleType,
    pub params: serde_json::Value,
    pub enabled: bool,
    pub priority: i32,
    pub valid_from: DateTime<Utc>,
    pub valid_until: Option<DateTime<Utc>>,
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
}

#[cfg(test)]
mod tests {
    use super::*;

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
