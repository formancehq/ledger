--statement
CREATE INDEX IF NOT EXISTS transactions_ts_desc ON "VAR_LEDGER_NAME".transactions ("timestamp" DESC);