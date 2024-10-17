alter table "{{.Bucket}}".transactions_metadata
add column transactions_id bigint;

update "{{.Bucket}}".transactions_metadata
set transactions_id = (
	select id
	from "{{.Bucket}}".transactions
	where transactions_metadata.transactions_seq = transactions.seq
);

alter table "{{.Bucket}}".transactions_metadata
drop column transactions_seq;

alter table "{{.Bucket}}".transactions_metadata
alter column transactions_id
set not null;