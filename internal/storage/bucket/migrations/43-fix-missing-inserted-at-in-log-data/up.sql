do $$
	declare
		_offset integer := 0;
		_batch_size integer := 1000;
	begin
		set search_path = '{{ .Schema }}';

		create temp table logs_view as
			select row_number() over (order by id) as row_number, id, ledger
			from logs
			where type = 'NEW_TRANSACTION' or type = 'REVERTED_TRANSACTION';
		create index logs_view_row_numbers on logs_view(row_number);

		perform pg_notify('migrations-{{ .Schema }}', 'init: ' || (select count(*) from logs_view));

		loop
			with _rows as (
				select id, ledger, row_number
				from logs_view
				where row_number >= _offset and row_number < _offset + _batch_size
			)
			update logs
			set data = jsonb_set(data, '{transaction, insertedAt}', to_jsonb(to_jsonb(date)#>>'{}' || 'Z'))
			from _rows
			where logs.id = _rows.id and
			      logs.ledger = _rows.ledger;

			exit when not found;

			_offset = _offset + _batch_size;

			perform pg_notify('migrations-{{ .Schema }}', 'continue: ' || _batch_size);

			commit;
		end loop;

		drop table if exists logs_view;
	end
$$;

