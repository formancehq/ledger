do $$
	declare
		ledger record;
		vsql varchar;
	begin
		set search_path = '{{ .Schema }}';

		for ledger in select * from _system.ledgers where bucket = current_schema loop
			vsql = 'ALTER TABLE transactions REPLICA IDENTITY USING INDEX transactions_ledger';
			execute vsql;

			vsql = 'ALTER TABLE accounts REPLICA IDENTITY USING INDEX accounts_ledger';
			execute vsql;

			vsql = 'ALTER TABLE logs REPLICA IDENTITY USING INDEX logs_ledger';
			execute vsql;
		end loop;
	end
$$;

