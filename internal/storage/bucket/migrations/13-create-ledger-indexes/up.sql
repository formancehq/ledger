set search_path = '{{.Schema}}';

drop index transactions_reference;
alter index transactions_reference2 rename to transactions_reference;

DO
$do$
	declare
		ledger record;
		vsql text;
	BEGIN
		for ledger in select * from _system.ledgers where bucket = current_schema loop
			-- enable post commit effective volumes synchronously
			vsql = 'create index "pcev_' || ledger.id || '" on moves (accounts_address, asset, effective_date desc) where ledger = ''' || ledger.name || '''';
			execute vsql;

			vsql = 'create index "transactions_sources_' || ledger.id || '" on transactions using gin (sources jsonb_path_ops) where ledger = ''' || ledger.name || '''';
			execute vsql;

			vsql = 'create index "transactions_destinations_' || ledger.id || '" on transactions using gin (destinations jsonb_path_ops) where ledger = ''' || ledger.name || '''';
			execute vsql;

			vsql = 'create index "accounts_address_array_' || ledger.id || '" on accounts using gin (address_array jsonb_ops) where ledger = ''' || ledger.name || '''';
			execute vsql;

			vsql = 'create index "accounts_address_array_length_' || ledger.id || '" on accounts (jsonb_array_length(address_array)) where ledger = ''' || ledger.name || '''';
			execute vsql;

			vsql = 'create index "transactions_sources_arrays_' || ledger.id || '" on transactions using gin (sources_arrays jsonb_path_ops) where ledger = ''' || ledger.name || '''';
			execute vsql;

			vsql = 'create index "transactions_destinations_arrays_' || ledger.id || '" on transactions using gin (destinations_arrays jsonb_path_ops) where ledger = ''' || ledger.name || '''';
			execute vsql;
		end loop;
	END
$do$;
