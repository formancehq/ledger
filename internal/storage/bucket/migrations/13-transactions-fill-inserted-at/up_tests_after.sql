set search_path = '{{.Bucket}}';

do $$
begin
	assert (
		select bool_and(log.date = transactions.inserted_at)
		from logs log
		join transactions on transactions.id = log.id
    ), 'Insertion dates of logs and transactions should not match';
end$$;