do $$
	declare
		ledger record;
		vsql varchar;
	begin
		set search_path = '{{ .Schema }}';

		for ledger in select * from _system.ledgers where bucket = current_schema loop
			vsql = 'drop trigger if exists "transaction_set_addresses_' || ledger.id || '" on transactions';
			execute vsql;

			vsql = 'drop trigger if exists "accounts_set_address_array_' || ledger.id || '" on accounts';
			execute vsql;

			vsql = 'drop trigger if exists "transaction_set_addresses_segments_' || ledger.id || '" on transactions';
			execute vsql;
		end loop;

		drop function set_transaction_addresses;
		drop function set_transaction_addresses_segments;
		drop function set_address_array_for_account;
		drop function explode_address;
	end
$$;