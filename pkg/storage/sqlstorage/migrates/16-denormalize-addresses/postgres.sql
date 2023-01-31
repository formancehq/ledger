--statement
alter table "VAR_LEDGER_NAME".transactions add column sources text;
--statement
alter table "VAR_LEDGER_NAME".transactions add column destinations text;
--statement
create index transactions_sources ON "VAR_LEDGER_NAME".transactions USING GIN (sources gin_trgm_ops);
--statement
create index transactions_destinations ON "VAR_LEDGER_NAME".transactions USING GIN (destinations gin_trgm_ops);
--statement
update "VAR_LEDGER_NAME".transactions
set sources = (
    select string_agg(ele->>'source', ';')
    from "VAR_LEDGER_NAME".transactions sub
    cross join lateral jsonb_array_elements(postings) source(ele)
    where transactions.id = sub.id
), destinations = (
    select string_agg(ele->>'destination', ';')
    from "VAR_LEDGER_NAME".transactions sub
    cross join lateral jsonb_array_elements(postings) source(ele)
    where transactions.id = sub.id
);
