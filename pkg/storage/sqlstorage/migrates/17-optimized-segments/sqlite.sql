--statement
drop table if exists metadata;
drop view if exists addresses;
drop index if exists p_c0;
drop index if exists posting_source;
drop index if exists posting_destination;
drop index if exists posting_asset;
drop index if exists m_i0;
drop table if exists postings;

--statement
drop trigger if exists new_transaction;
drop trigger if exists new_log_transaction;
drop trigger if exists new_log_set_metadata_on_transaction;
drop trigger if exists new_log_set_metadata_on_account;

--statement
alter table transactions drop column sources;
alter table transactions drop column destinations;

--statement
create table if not exists postings (
    txid bigint,
    posting_index integer,
    source jsonb,
    destination jsonb
);
