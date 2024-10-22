do $$
	declare
		_batch_size integer := 30;
		_date timestamp without time zone;
		_count integer;
	begin
		set search_path = '{{ .Bucket }}';

		-- select the date where the "11-make-stateless" migration has been applied
		select tstamp into _date
		from _system.goose_db_version
		where version_id = 12;

		select count(*) into _count
		from logs
		where date <= _date;
		
		for i in 0.._count by _batch_size loop
			update transactions
			set inserted_at = (
				select date
				from logs
				where transactions.id = (data->'transaction'->>'id')::bigint and transactions.ledger = ledger
			)
			where id >= i and id < i + _batch_size;

			commit;
		end loop;
	end
$$;