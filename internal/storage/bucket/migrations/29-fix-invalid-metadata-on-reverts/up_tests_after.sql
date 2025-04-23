do $$
	declare
		expected varchar = '{"tax": "1%", "com.formance.spec/state/reverts": "2"}';
	begin
		set search_path = '{{.Schema}}';
		assert (select metadata::varchar from transactions where id = 22 and ledger = 'ledger0') = expected,
			'metadata should be equals to ' || expected || ' but was ' || (select to_jsonb(metadata) from transactions where id = 22 and ledger = 'ledger0');
	end;
$$