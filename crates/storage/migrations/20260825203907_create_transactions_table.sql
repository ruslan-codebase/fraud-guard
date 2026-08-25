CREATE TABLE transactions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    transaction_id UUID NOT NULL UNIQUE,
    source_account TEXT NOT NULL,
    destination_account TEXT NOT NULL,
    amount BIGINT NOT NULL,
    currency TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_transactions_source_account_timestamp ON transactions(source_account, timestamp);
