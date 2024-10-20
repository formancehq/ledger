set search_path = '{{.Bucket}}';

do $$
begin
	assert (
		select count(*)
		from transactions
		where post_commit_volumes is null
	) = 0, 'Post commit volumes should be set on all transactions';
	assert (
		select post_commit_volumes
		from transactions
		where ledger = 'ledger0' and id = 1
	) = ('{'
	     '"fees": {'
	        '"USD": {'
	            '"inputs": 2, '
	            '"outputs": 0'
	        '}'
	     '}, '
	     '"world": {'
	        '"USD": {'
	            '"inputs": 0, '
	            '"outputs": 200'
	        '}'
	     '}, '
	     '"orders:5": {'
	        '"USD": {'
	            '"inputs": 100, '
	            '"outputs": 100'
	        '}'
	     '}, '
	     '"sellers:0": {'
	        '"USD": {'
	            '"inputs": 198, '
	            '"outputs": 0'
	        '}'
	     '}'
     '}')::jsonb, 'Post commit volumes should be correct';
end$$;