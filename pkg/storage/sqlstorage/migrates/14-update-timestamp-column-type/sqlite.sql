--statement
create table tmp_transactions as select * from transactions;
--statement
drop table transactions;
--statement
create table transactions (
    "id"        integer,
    "timestamp" date,
    "reference" varchar,
    "postings" varchar,
    "metadata" varchar,
    "pre_commit_volumes" varchar,
    "post_commit_volumes" varchar,

    unique("id"),
    unique("reference")
);
--statement
insert into transactions(id, timestamp, reference, postings, metadata, pre_commit_volumes, post_commit_volumes)
select id, timestamp, reference, postings, metadata, pre_commit_volumes, post_commit_volumes from tmp_transactions;
--statement
drop table tmp_transactions;
