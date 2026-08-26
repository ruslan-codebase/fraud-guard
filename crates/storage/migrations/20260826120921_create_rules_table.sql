CREATE TABLE rules (
    code TEXT PRIMARY KEY,
    rule_type TEXT NOT NULL,
    params JSONB NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT true,
    priority INT NOT NULL DEFAULT 0,
    valid_from TIMESTAMPTZ NOT NULL,
    valid_until TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_rules_enabled ON rules(enabled);
CREATE INDEX idx_rules_valid_period ON rules(valid_from, valid_until);
CREATE INDEX idx_rules_active ON rules(enabled, valid_from, valid_until);
