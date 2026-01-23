do $$
	begin
		set search_path = '{{ .Schema }}';

		ALTER TABLE transactions REPLICA IDENTITY USING INDEX transactions_ledger;
		ALTER TABLE accounts REPLICA IDENTITY USING INDEX accounts_ledger;
		ALTER TABLE logs REPLICA IDENTITY USING INDEX logs_ledger;
	end
$$;

