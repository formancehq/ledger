--statement
alter table transactions drop column sources;
--statement
alter table transactions drop column destinations;

--statement
drop table if exists postings;

--statement
create table if not exists postings (
    txid bigint,
    postingIndex integer,
    source jsonb,
    destination jsonb
);
