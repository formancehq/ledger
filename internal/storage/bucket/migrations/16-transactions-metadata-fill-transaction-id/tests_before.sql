set search_path = '{{.Bucket}}';

do $$
begin
	assert (
		select count(*)
		from transactions_metadata
		where transactions_id is null
    ) > 0, 'Transactions ids of transactions_metadata table should be null';
end$$;