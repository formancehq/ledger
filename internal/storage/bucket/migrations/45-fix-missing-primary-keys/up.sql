do $$
	begin
		set search_path = '{{ .Schema }}';

		ALTER TABLE transactions ADD PRIMARY KEY USING INDEX transactions_ledger;
		ALTER TABLE accounts ADD PRIMARY KEY USING INDEX accounts_ledger;
		ALTER TABLE logs ADD PRIMARY KEY USING INDEX logs_ledger;
	end
$$;

