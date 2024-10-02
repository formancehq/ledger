alter table "{{.Bucket}}".moves
add column transactions_id bigint;

update "{{.Bucket}}".moves
set transactions_id = (
	select id
	from "{{.Bucket}}".transactions
	where seq = transactions_seq
);

alter table "{{.Bucket}}".moves
alter column transactions_id set not null;

alter table "{{.Bucket}}".moves
drop column transactions_seq;