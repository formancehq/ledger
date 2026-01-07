create or replace function update_transaction_metadata_history() returns trigger
	security definer
	language plpgsql
as
$$
begin
	insert into transactions_metadata (ledger, transactions_id, revision, date, metadata)
	values (new.ledger, new.id, coalesce((
         select revision + 1
         from transactions_metadata
         where transactions_metadata.transactions_id = new.id::bigint and transactions_metadata.ledger = new.ledger
         order by revision desc
         limit 1
     ), 1), new.updated_at, new.metadata);

	return new;
end;
$$ set search_path = '{{.Schema}}';