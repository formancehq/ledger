alter table moves
add column transactions_id bigint;

update moves
set transactions_id = (
	select id
	from transactions
	where seq = transactions_seq
);

alter table moves
alter column transactions_id set not null;

alter table moves
drop column transactions_seq;