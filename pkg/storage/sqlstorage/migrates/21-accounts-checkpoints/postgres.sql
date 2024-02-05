--statement
CREATE TABLE IF NOT EXISTS "VAR_LEDGER_NAME".accounts_checkpoints (
    txid BIGINT,
    account TEXT,
    last_tx_at TIMESTAMPTZ NOT NULL,

    UNIQUE (account, last_tx_at)
);