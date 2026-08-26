use async_trait::async_trait;
use fraud_guard_domain::{DomainError, Rule, RuleRepository, RuleType};
use sqlx::PgPool;
use std::str::FromStr;

pub struct PostgresRuleRepository {
    pool: PgPool,
}

impl PostgresRuleRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl RuleRepository for PostgresRuleRepository {
    async fn load_active_rules(&self) -> Result<Vec<Rule>, DomainError> {
        let rows = sqlx::query!(
            r#"
            SELECT
                code,
                rule_type,
                params,
                enabled,
                priority,
                valid_from,
                valid_until
            FROM rules
            WHERE enabled = true
              AND valid_from <= NOW()
              AND (valid_until IS NULL OR valid_until > NOW())
            ORDER BY priority DESC
            "#
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| DomainError::Repository {
            operation: "load active rules from DB",
            message: e.to_string(),
        })?;

        let mut rules = Vec::with_capacity(rows.len());
        for row in rows {
            let rule_type =
                RuleType::from_str(&row.rule_type).map_err(|e| DomainError::Repository {
                    operation: "parse rule type from DB",
                    message: e.to_string(),
                })?;

            let rule = Rule {
                code: row.code,
                rule_type,
                params: row.params,
                enabled: row.enabled,
                priority: row.priority,
                valid_from: row.valid_from,
                valid_until: row.valid_until,
            };
            rules.push(rule);
        }
        Ok(rules)
    }
}
