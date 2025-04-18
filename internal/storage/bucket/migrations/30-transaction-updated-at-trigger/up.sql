set search_path = '{{ .Schema }}';

create or replace function set_transaction_updated_at() returns trigger
	security definer
	language plpgsql
as
$$
begin
	new.updated_at = new.inserted_at;

	return new;
end
$$ set search_path from current;

create trigger set_transaction_updated_at
	before insert on transactions
	for each row
	when ( new.updated_at is null )
execute procedure set_transaction_updated_at();

alter table transactions
add constraint transactions_updated_at_not_null
check (updated_at is not null)
not valid;