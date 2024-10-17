alter table "{{.Bucket}}".accounts_metadata
add column accounts_address varchar;

update "{{.Bucket}}".accounts_metadata
set accounts_address = (
	select address
	from "{{.Bucket}}".accounts
	where accounts_metadata.accounts_seq = seq
);

alter table "{{.Bucket}}".accounts_metadata
drop column accounts_seq;

alter table "{{.Bucket}}".accounts_metadata
alter column accounts_address set not null;