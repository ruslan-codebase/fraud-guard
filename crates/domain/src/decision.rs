#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TriggeredRule {
    pub code: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decision {
    Accept,
    Review { triggered_rules: Vec<TriggeredRule> },
    Decline { reason: String },
}
