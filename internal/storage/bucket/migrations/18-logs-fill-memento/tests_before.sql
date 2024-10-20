set search_path = '{{.Bucket}}';

do $$
begin
	assert (
		select count(*)
		from logs
		where memento is null
    ) > 0, 'Mementos of logs table should be null';
end$$;