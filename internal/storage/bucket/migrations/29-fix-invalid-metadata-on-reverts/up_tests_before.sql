do $$
	declare
		expected varchar = '{"tax": "1%"}';
	begin
		set search_path = '{{.Schema}}';

		insert into transactions (
			ledger,
			id,
			inserted_at,
			postings,
			post_commit_volumes
		) values (
			'ledger0',
			nextval('transaction_id_1'),
			now(),
			'[{"source": "world", "destination": "bank", "asset": "USD", "amount": 100}]',
			'{}'
		);

		assert (select metadata::varchar from transactions where id = 22 and ledger = 'ledger0') = expected,
			'metadata should be equals to ' || expected || ' but was ' || (select to_jsonb(metadata) from transactions where id = 22 and ledger = 'ledger0');
	end;
$$