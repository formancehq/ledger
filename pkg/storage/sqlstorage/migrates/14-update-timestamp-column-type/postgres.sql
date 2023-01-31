--statement
ALTER TABLE "VAR_LEDGER_NAME".transactions ADD COLUMN timestamp_holder timestamptz NULL;
--statement
UPDATE "VAR_LEDGER_NAME".transactions SET timestamp_holder = timestamp::TIMESTAMP;
--statement
ALTER TABLE "VAR_LEDGER_NAME".transactions ALTER COLUMN timestamp TYPE timestamptz USING timestamp_holder;
--statement
ALTER TABLE "VAR_LEDGER_NAME".transactions DROP COLUMN timestamp_holder;
