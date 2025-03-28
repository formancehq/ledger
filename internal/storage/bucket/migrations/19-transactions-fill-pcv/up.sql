do $$
	declare
		_offset integer := 0;
		_batch_size integer := 1000;
	begin
		set search_path = '{{ .Schema }}';

		drop table if exists moves_view;

		create temp table moves_view as
		select transactions_seq, public.aggregate_objects(jsonb_build_object(accounts_address, volumes)) as volumes
		from (
			select transactions_seq::numeric, accounts_address, public.aggregate_objects(json_build_object(asset, json_build_object('input', (post_commit_volumes).inputs, 'output', (post_commit_volumes).outputs))::jsonb) as volumes
			from (
				SELECT DISTINCT ON (moves.transactions_seq, accounts_address, asset) moves.transactions_seq, accounts_address, asset,
							first_value(post_commit_volumes) OVER (
					PARTITION BY moves.transactions_seq, accounts_address, asset
					ORDER BY seq DESC
				) AS post_commit_volumes
				FROM moves
				where insertion_date < (
					select tstamp from goose_db_version where version_id = 12
				)
			) moves
			group by transactions_seq, accounts_address
		) data
		group by transactions_seq;

		create index moves_view_idx on moves_view(transactions_seq);

		if (select count(*) from moves_view) = 0 then
			return;
		end if;

		perform pg_notify('migrations-{{ .Schema }}', 'init: ' || (select count(*) from moves_view));

		loop
			with data as (
				select transactions_seq, volumes
				from moves_view
				-- play better than offset/limit
				where transactions_seq >= _offset and transactions_seq < _offset + _batch_size
			)
			update transactions
			set post_commit_volumes = data.volumes
			from data
			where transactions.seq = data.transactions_seq;

			exit when not found;

			_offset = _offset + _batch_size;

			perform pg_notify('migrations-{{ .Schema }}', 'continue: ' || _batch_size);

			commit;
		end loop;

		drop table if exists moves_view;

 		alter table transactions
 		add constraint post_commit_volumes_not_null
 		check (post_commit_volumes is not null)
 		not valid;
	end
$$;

