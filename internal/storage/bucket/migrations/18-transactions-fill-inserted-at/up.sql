do $$
	declare
		_batch_size integer := 100;
		_date timestamp without time zone;
		_count integer := 0;
	begin
		--todo: take explicit advisory lock to avoid concurrent migrations when the service is killed
		set search_path = '{{.Schema}}';

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

		for i in 0.._count by _batch_size loop
			-- disable triggers
			set session_replication_role = replica;

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

			-- enable triggers
			set session_replication_role = default;

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