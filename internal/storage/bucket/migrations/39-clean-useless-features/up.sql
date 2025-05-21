do $$
	declare
		ledger record;
		vsql varchar;
	begin
		set search_path = '{{ .Schema }}';

		-- recreate trigger but with a check on sources is null for transactions and address_array is null for accounts
		-- this way, the 2.2 (which use triggers) continuer to work, and the 2.3 which does not need them can work too
		-- todo(next minor / 2.4): remove triggers and associated functions
		for ledger in select * from _system.ledgers where bucket = current_schema loop
			vsql = 'create or replace trigger "transaction_set_addresses_' || ledger.id || '" before insert on transactions for each row when (new.ledger = ''' || ledger.name || ''' and new.sources is null) execute procedure set_transaction_addresses()';
			execute vsql;

			vsql = 'create or replace trigger "accounts_set_address_array_' || ledger.id || '" before insert on accounts for each row when (new.ledger = ''' || ledger.name || ''' and new.address_array is null) execute procedure set_address_array_for_account()';
			execute vsql;

			vsql = 'create or replace trigger "transaction_set_addresses_segments_' || ledger.id || '"	before insert on "transactions" for each row when (new.ledger = ''' || ledger.name || ''' and new.sources_arrays is null) execute procedure set_transaction_addresses_segments()';
			execute vsql;
		end loop;

		-- todo(next minor / 2.4): remove triggers and associated functions
-- 		for ledger in select * from _system.ledgers where bucket = current_schema loop
-- 			vsql = 'drop trigger if exists "transaction_set_addresses_' || ledger.id || '" on transactions';
-- 			execute vsql;
--
-- 			vsql = 'drop trigger if exists "accounts_set_address_array_' || ledger.id || '" on accounts';
-- 			execute vsql;
--
-- 			vsql = 'drop trigger if exists "transaction_set_addresses_segments_' || ledger.id || '" on transactions';
-- 			execute vsql;
-- 		end loop;
--
-- 		commit;
--
-- 		drop function set_transaction_addresses;
-- 		drop function set_transaction_addresses_segments;
-- 		drop function set_address_array_for_account;
-- 		drop function explode_address;
	end
$$;

