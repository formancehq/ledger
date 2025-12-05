do $$
	declare
		_offset integer := 0;
		_batch_size integer := 1000;
	begin
		set search_path = '{{ .Schema }}';

		drop table if exists txs_view;

		create table txs_view as
		select row_number() over (order by transactions.seq) as row_number, *
		from transactions
		where updated_at is null;

		if (select count(*) from txs_view) = 0 then
			drop table txs_view;
			return;
		end if;
		-- speed up hash join when updating rows later
		create index txs_view_seq_idx on txs_view(seq);
		alter table txs_view add foreign key(seq) references transactions(seq);

		perform pg_notify('migrations-{{ .Schema }}', 'init: ' || (select count(*) from txs_view));

		loop
			with data as (
				select *
				from txs_view
				where row_number > _offset and row_number <= _offset+_batch_size
			)
			update transactions
			set updated_at = transactions.inserted_at
			from data
			where transactions.seq = data.seq;

			if not found then
				drop table txs_view;
				exit;
			end if;

			_offset = _offset + _batch_size;

			perform pg_notify('migrations-{{ .Schema }}', 'continue: ' || _batch_size);

			commit;
		end loop;

		drop table if exists txs_view;
	end
$$;

