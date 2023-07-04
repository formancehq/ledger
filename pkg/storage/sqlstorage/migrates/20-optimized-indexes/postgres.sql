--statement
CREATE INDEX IF NOT EXISTS postings_array_length_src ON "VAR_LEDGER_NAME".postings (jsonb_array_length(source));
--statement
CREATE INDEX IF NOT EXISTS postings_array_length_dst ON "VAR_LEDGER_NAME".postings (jsonb_array_length(destination));
--statement
CREATE INDEX IF NOT EXISTS accounts_array_length ON "VAR_LEDGER_NAME".accounts (jsonb_array_length(address_json));
--statement
CREATE INDEX IF NOT EXISTS volumes_array_length ON "VAR_LEDGER_NAME".volumes (jsonb_array_length(account_json));
