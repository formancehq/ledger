--statement
drop trigger if exists log_entry on "VAR_LEDGER_NAME".log;
--statement
drop trigger if exists volumes_changed on "VAR_LEDGER_NAME".log;

--statement
alter table "VAR_LEDGER_NAME".transactions drop column if exists sources;
--statement
alter table "VAR_LEDGER_NAME".transactions drop column if exists destinations;
--statement
drop index if exists transactions_sources;
--statement
drop index if exists transactions_destinations;

--statement
create table if not exists "VAR_LEDGER_NAME".postings (
    txid bigint,
    posting_index integer,
    source jsonb,
    destination jsonb
);

--statement
create index if not exists postings_src on "VAR_LEDGER_NAME".postings using GIN(source);
--statement
create index if not exists postings_dest on "VAR_LEDGER_NAME".postings using GIN(destination);
--statement
create index if not exists postings_txid on "VAR_LEDGER_NAME".postings (txid asc);

--statement
insert into "VAR_LEDGER_NAME".postings(txid, posting_index, source, destination)
select txs.id as txid, i - 1 as posting_index,
    array_to_json(string_to_array(t.posting->>'source', ':'))::jsonb as source,
    array_to_json(string_to_array(t.posting->>'destination', ':'))::jsonb as destination
from "VAR_LEDGER_NAME".transactions txs left join lateral jsonb_array_elements(txs.postings)
with ordinality as t(posting, i) on true;
