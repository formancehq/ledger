alter table accounts_metadata
add column accounts_address varchar;

update accounts_metadata
set accounts_address = (
	select address
	from accounts
	where accounts_metadata.accounts_seq = seq
);

alter table accounts_metadata
drop column accounts_seq;

alter table accounts_metadata
alter column accounts_address set not null;