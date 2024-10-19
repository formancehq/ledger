do $$
	declare
		_batch_size integer := 30;
		_date timestamp without time zone;
		_count integer;
	begin
		set search_path = '{{.Schema}}';

		-- select the date where the "11-make-stateless" migration has been applied
		select tstamp into _date
		from _system.goose_db_version
		where version_id = 12;

		select count(*) into _count
		from logs
		where date <= _date;

		perform pg_notify('migrations-{{ .Schema }}', 'init: ' || _count);

		for i in 0.._count by _batch_size loop
			update transactions
			set inserted_at = (
				select date
				from logs
				where transactions.id = (data->'transaction'->>'id')::bigint and transactions.ledger = ledger
			)
			where id >= i and id < i + _batch_size;

			commit;

			perform pg_notify('migrations-{{ .Schema }}', 'continue: 1');

		end loop;
	end
$$;