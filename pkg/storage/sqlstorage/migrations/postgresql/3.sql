--statement
CREATE INDEX IF NOT EXISTS account_address ON "VAR_LEDGER_NAME".accounts ("address");
--statement
CREATE OR REPLACE FUNCTION "VAR_LEDGER_NAME".use_account(postings jsonb, account varchar)
    RETURNS BOOLEAN
AS
$$
DECLARE
p jsonb;
BEGIN
FOR p IN (
        SELECT jsonb_array_elements(postings)
    ) LOOP
        IF p->>'source' = account THEN RETURN true; END IF;
        IF p->>'destination' = account THEN RETURN true; END IF;
END LOOP;
RETURN false;
END
$$
LANGUAGE plpgsql
IMMUTABLE;