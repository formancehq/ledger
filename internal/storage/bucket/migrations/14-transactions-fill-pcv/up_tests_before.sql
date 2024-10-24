set search_path = '{{.Bucket}}';

do $$
begin
	assert (
		select count(*)
		from transactions
		where post_commit_volumes is null
    ) > 0, 'Post commit volumes should be null on all transactions';
end$$;