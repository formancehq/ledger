DO
$do$
	declare
		ledger record;
		vsql text;
	BEGIN
		for ledger in select * from _system.ledgers where bucket = '{{.Bucket}}' loop
			-- create a sequence for transactions by ledger instead of a sequence of the table as we want to have contiguous ids
			-- notes: we can still have "holes" on ids since a sql transaction can be reverted after a usage of the sequence

			vsql = 'create sequence "{{.Bucket}}"."transaction_id_' || ledger.id || '" owned by "{{.Bucket}}".transactions.id';
			execute vsql;

			vsql = 'select setval("{{.Bucket}}"."transaction_id_' || ledger.id || '", coalesce((select max(id) + 1 from "{{.Bucket}}".transactions where ledger = ledger.name), 1)::bigint, false)';
			execute vsql;

			-- create a sequence for logs by ledger instead of a sequence of the table as we want to have contiguous ids
			-- notes: we can still have "holes" on id since a sql transaction can be reverted after a usage of the sequence
			vsql = 'create sequence "{{.Bucket}}"."log_id_' || ledger.id || '" owned by "{{.Bucket}}".logs.id';
			execute vsql;

			vsql = 'select setval("{{.Bucket}}"."log_id_' || ledger.id || '", coalesce((select max(id) + 1 from "{{.Bucket}}".logs where ledger = ledger.name), 1)::bigint, false)';
			execute vsql;

			-- enable post commit effective volumes synchronously
			vsql = 'create index "pcev_' || ledger.id || '" on "{{.Bucket}}".moves (accounts_address, asset, effective_date desc) where ledger = ledger.name';
			execute vsql;

			vsql = 'create trigger "set_effective_volumes_' || ledger.id || '" before insert on "{{.Bucket}}".moves for each row when (new.ledger = ledger.name) execute procedure "{{.Bucket}}".set_effective_volumes()';
			execute vsql;

			vsql = 'create trigger "update_effective_volumes_' || ledger.id || '" after insert on "{{.Bucket}}".moves for each row when (new.ledger = ledger.name) execute procedure "{{.Bucket}}".update_effective_volumes()';
			execute vsql;

			-- logs hash
			vsql = 'create trigger "set_log_hash_' || ledger.id || '" before insert on "{{.Bucket}}".logs for each row when (new.ledger = ledger.name) execute procedure "{{.Bucket}}".set_log_hash()';
			execute vsql;

			vsql = 'create trigger "update_account_metadata_history_' || ledger.id || '" after update on "{{.Bucket}}"."accounts" for each row when (new.ledger = ledger.name) execute procedure "{{.Bucket}}".update_account_metadata_history()';
			execute vsql;

			vsql = 'create trigger "insert_account_metadata_history_' || ledger.id || '" after insert on "{{.Bucket}}"."accounts" for each row when (new.ledger = ledger.name) execute procedure "{{.Bucket}}".insert_account_metadata_history()';
			execute vsql;

			vsql = 'create trigger "update_transaction_metadata_history_' || ledger.id || '" after update on "{{.Bucket}}"."transactions" for each row when (new.ledger = ledger.name) execute procedure "{{.Bucket}}".update_transaction_metadata_history()';
			execute vsql;

			vsql = 'create trigger "insert_transaction_metadata_history_' || ledger.id || '" after insert on "{{.Bucket}}"."transactions" for each row when (new.ledger = ledger.name) execute procedure "{{.Bucket}}".insert_transaction_metadata_history()';
			execute vsql;

			vsql = 'create index "transactions_sources_' || ledger.id || '" on "{{.Bucket}}".transactions using gin (sources jsonb_path_ops) where ledger = ledger.name';
			execute vsql;

			vsql = 'create index "transactions_destinations_' || ledger.id || '" on "{{.Bucket}}".transactions using gin (destinations jsonb_path_ops) where ledger = ledger.name';
			execute vsql;

			vsql = 'create trigger "transaction_set_addresses_' || ledger.id || '" before insert on "{{.Bucket}}".transactions for each row when (new.ledger = ledger.name) execute procedure "{{.Bucket}}".set_transaction_addresses()';
			execute vsql;

			vsql = 'create index "accounts_address_array_' || ledger.id || '" on "{{.Bucket}}".accounts using gin (address_array jsonb_ops) where ledger = ledger.name';
			execute vsql;

			vsql = 'create index "accounts_address_array_length_' || ledger.id || '" on "{{.Bucket}}".accounts (jsonb_array_length(address_array)) where ledger = ledger.name';
			execute vsql;

			vsql = 'create trigger "accounts_set_address_array_' || ledger.id || '" before insert on "{{.Bucket}}".accounts for each row when (new.ledger = ledger.name) execute procedure "{{.Bucket}}".set_address_array_for_account()';
			execute vsql;

			vsql = 'create index "transactions_sources_arrays_' || ledger.id || '" on "{{.Bucket}}".transactions using gin (sources_arrays jsonb_path_ops) where ledger = ledger.name';
			execute vsql;

			vsql = 'create index "transactions_destinations_arrays_' || ledger.id || '" on "{{.Bucket}}".transactions using gin (destinations_arrays jsonb_path_ops) where ledger = ledger.name';
			execute vsql;

			vsql = 'create trigger "transaction_set_addresses_segments_' || ledger.id || '"	before insert on "{{.Bucket}}"."transactions" for each row when (new.ledger = ledger.name) execute procedure "{{.Bucket}}".set_transaction_addresses_segments()';
			execute vsql;
		end loop;
	END
$do$;