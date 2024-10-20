set search_path = '{{.Bucket}}';

do $$
begin
	assert (
		select count(*)
		from transactions_metadata
		where transactions_id is null
	) = 0, 'Transactions ids on transactions_metadata table should not be null';
end$$;