do $$
	declare
		_ledger record;
		_vsql text;
		_batch_size integer := 1000;
		_date timestamp without time zone;
		_count integer := 0;
	begin
		set search_path = '{{.Schema}}';

		-- cannot disable triggers at session level on Azure Postgres with no superuser privileges.
		-- so we modify the trigger acting on transaction update to be triggered only if the metadata column is updated.
		-- by the way, it's a good move to not trigger the update_transaction_metadata_history function on every update if not necessary.
		for _ledger in select * from _system.ledgers where bucket = current_schema loop
			_vsql = 'drop trigger if exists "update_transaction_metadata_history_' || _ledger.id || '" on "transactions"';
			execute _vsql;

			_vsql = 'create trigger "update_transaction_metadata_history_' || _ledger.id || '" after update of metadata on "transactions" for each row when (new.ledger = ''' || _ledger.name || ''') execute procedure update_transaction_metadata_history()';
			execute _vsql;
		end loop;

		-- select the date where the "11-make-stateless" migration has been applied
		select tstamp into _date
		from _system.goose_db_version
		where version_id = 12;

		create temporary table logs_transactions as
		select id, ledger, date, (data->'transaction'->>'id')::bigint as transaction_id
		from logs
		where date <= _date;

		create index on logs_transactions (ledger, transaction_id) include (id, date);

		select count(*) into _count
		from logs_transactions;

		perform pg_notify('migrations-{{ .Schema }}', 'init: ' || _count);

		for i in 0.._count-1 by _batch_size loop
			with _rows as (
				select *
				from logs_transactions
				order by ledger, transaction_id
				offset i
				limit _batch_size
			)
			update transactions
			set inserted_at = _rows.date
			from _rows
			where transactions.ledger = _rows.ledger and transactions.id = _rows.transaction_id;

			commit;

			perform pg_notify('migrations-{{ .Schema }}', 'continue: ' || _batch_size);
		end loop;

		drop table logs_transactions;

		alter table transactions
		alter column inserted_at set default transaction_date();

		drop trigger set_transaction_inserted_at on transactions;
		drop function set_transaction_inserted_at;
	end
$$;

do $$
	declare
		_ledger record;
		_vsql text;
	begin
		-- cannot disable triggers at session level on Azure Postgres with no superuser privileges.
		-- so we modify the trigger acting on transaction update to be triggered only if the metadata column is updated.
		-- by the way, it's a good move to not trigger the update_transaction_metadata_history function on every update if not necessary.
		for _ledger in select * from _system.ledgers where bucket = current_schema loop
			_vsql = 'create or replace trigger "update_transaction_metadata_history_' || _ledger.id || '" after update of metadata on "transactions" for each row when (new.ledger = ''' || _ledger.name || ''') execute procedure update_transaction_metadata_history()';
			execute _vsql;
		end loop;
	end
$$;