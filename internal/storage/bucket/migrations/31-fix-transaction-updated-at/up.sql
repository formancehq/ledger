do $$
	declare
		_offset integer := 0;
		_batch_size integer := 1000;
	begin
		set search_path = '{{ .Schema }}';

		drop table if exists txs_view;

		create temp table txs_view as
		select *
		from transactions
		where updated_at is null;

		if (select count(*) from txs_view) = 0 then
			return;
		end if;

		perform pg_notify('migrations-{{ .Schema }}', 'init: ' || (select count(*) from txs_view));

		loop
			with data as (
				select *
				from txs_view
				order by seq
				offset _offset
				limit _batch_size
			)
			update transactions
			set updated_at = transactions.inserted_at
			from data
			where transactions.seq = data.seq and
			      transactions.ledger = data.ledger;

			exit when not found;

			_offset = _offset + _batch_size;

			perform pg_notify('migrations-{{ .Schema }}', 'continue: ' || _batch_size);

			commit;
		end loop;

		drop table if exists txs_view;
	end
$$;

