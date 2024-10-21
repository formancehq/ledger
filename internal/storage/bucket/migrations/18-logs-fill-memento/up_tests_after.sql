set search_path = '{{.Bucket}}';

do $$
begin
	assert (
		select count(*)
		from logs
		where memento is null
	) = 0, 'Mememtos of logs table should not be null';
end$$;