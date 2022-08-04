--statement
ALTER TABLE "VAR_LEDGER_NAME".volumes
ALTER COLUMN input TYPE numeric(128, 0),
ALTER COLUMN output TYPE numeric(128, 0);