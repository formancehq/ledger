--statement
CREATE OR REPLACE FUNCTION "VAR_LEDGER_NAME".use_account_as_source(postings jsonb, account varchar)
    RETURNS BOOLEAN
AS $$
SELECT postings @> ('[{"source": "' || account || '"}]')::jsonb
$$ LANGUAGE sql;
--statement
CREATE OR REPLACE FUNCTION "VAR_LEDGER_NAME".use_account_as_destination(postings jsonb, account varchar)
    RETURNS BOOLEAN
AS $$
SELECT postings @> ('[{"destination": "' || account || '"}]')::jsonb
$$ LANGUAGE sql;