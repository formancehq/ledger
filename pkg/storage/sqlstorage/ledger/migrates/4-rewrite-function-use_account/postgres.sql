--statement
CREATE OR REPLACE FUNCTION "VAR_LEDGER_NAME".use_account(postings jsonb, account varchar)
    RETURNS BOOLEAN
AS $$
SELECT postings @> ('[{"destination": "' || account || '"}]')::jsonb OR postings @> ('[{"source": "' || account || '"}]')::jsonb
$$ LANGUAGE sql;
--statement
CREATE INDEX postings_addresses ON "VAR_LEDGER_NAME".transactions USING GIN (postings);
