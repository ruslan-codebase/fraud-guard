CREATE TABLE decisions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    transaction_id UUID NOT NULL UNIQUE,
    decision JSONB NOT NULL,
    evaluated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE decisions
    ADD CONSTRAINT fk_decisions_transaction
    FOREIGN KEY (transaction_id) REFERENCES transactions(transaction_id) ON DELETE CASCADE;

CREATE INDEX idx_decisions_transaction_id ON decisions(transaction_id);
