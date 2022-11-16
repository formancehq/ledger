--statement
alter table "VAR_LEDGER_NAME".transactions drop column sources;
--statement
alter table "VAR_LEDGER_NAME".transactions drop column destinations;
--statement
drop index if exists transactions_sources;
--statement
drop index if exists transactions_destinations;

--statement
drop table if exists postings;

--statement
create table if not exists "VAR_LEDGER_NAME".postings (
    txid bigint,
    postingIndex integer,
    source jsonb,
    destination jsonb
);

--statement
create index postingsSrc on "VAR_LEDGER_NAME".postings using GIN(source);
create index postingsDest on "VAR_LEDGER_NAME".postings using GIN(destination);
create index postingsTxid on "VAR_LEDGER_NAME".postings (txid asc);

--statement
insert into "VAR_LEDGER_NAME".postings(txid, postingIndex, source, destination)
select txs.id as txid,
    i - 1 as postingIndex,
    array_to_json(string_to_array(t.posting->>'source', ':'))::jsonb as source,
    array_to_json(string_to_array(t.posting->>'destination', ':'))::jsonb as destination
from "VAR_LEDGER_NAME".transactions txs left join lateral jsonb_array_elements(txs.postings)
with ordinality as t(posting, i) on true;
