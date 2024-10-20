set search_path = '{{.Bucket}}';

do $$
begin
	assert (
		select count(*)
		from accounts_metadata
		where accounts_address is null
	) = 0, 'Account addresses on accounts_metadata table should not be null';
end$$;