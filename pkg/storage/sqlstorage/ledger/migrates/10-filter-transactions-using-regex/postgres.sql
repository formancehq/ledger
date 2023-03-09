--statement
CREATE OR REPLACE FUNCTION "VAR_LEDGER_NAME".use_account_as_source(postings jsonb, account varchar)
    RETURNS BOOLEAN
AS $$
select bool_or(v.value::bool) from (
    select jsonb_extract_path_text(jsonb_array_elements(postings), 'source') ~ ('^' || account || '$') as value) as v;
$$ LANGUAGE sql;
--statement
CREATE OR REPLACE FUNCTION "VAR_LEDGER_NAME".use_account_as_destination(postings jsonb, account varchar)
    RETURNS BOOLEAN
AS $$
select bool_or(v.value::bool) from (
    select jsonb_extract_path_text(jsonb_array_elements(postings), 'destination') ~ ('^' || account || '$') as value) as v;
$$ LANGUAGE sql;
--statement
CREATE OR REPLACE FUNCTION "VAR_LEDGER_NAME".use_account(postings jsonb, account varchar)
    RETURNS BOOLEAN
AS
$$
SELECT bool_or(v.value) from (
     SELECT "VAR_LEDGER_NAME".use_account_as_source(postings, account) AS value UNION SELECT "VAR_LEDGER_NAME".use_account_as_destination(postings, account) AS value
) v
$$
LANGUAGE sql;
