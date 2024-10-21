set search_path = '{{.Bucket}}';

do $$
begin
	assert (
		select count(*)
		from moves
		where transactions_id is null
	) > 0, 'Should have some transactions with null transactions_id';
end$$;