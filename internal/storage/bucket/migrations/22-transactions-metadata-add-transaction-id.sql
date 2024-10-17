alter table transactions_metadata
add column transactions_id bigint;

update transactions_metadata
set transactions_id = (
	select id
	from transactions
	where transactions_metadata.transactions_seq = transactions.seq
);

alter table transactions_metadata
drop column transactions_seq;

alter table transactions_metadata
alter column transactions_id
set not null;