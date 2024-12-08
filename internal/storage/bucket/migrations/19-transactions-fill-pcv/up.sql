do $$
	declare
		_offset integer := 0;
		_batch_size integer := 1000;
	begin
		set search_path = '{{ .Schema }}';

		drop table if exists moves_view;

		create temp table moves_view as
		select transactions_id::numeric, public.aggregate_objects(json_build_object(accounts_address, json_build_object(asset, post_commit_volumes))::jsonb) as volumes
		from (
			SELECT DISTINCT ON (moves.transactions_id, accounts_address, asset) moves.transactions_id, accounts_address, asset,
						first_value(post_commit_volumes) OVER (
					PARTITION BY moves.transactions_id, accounts_address, asset
					ORDER BY seq DESC
					) AS post_commit_volumes
			FROM moves
			where insertion_date < (
				select tstamp from goose_db_version where version_id = 12
			)
		) moves
		group by transactions_id;

		perform pg_notify('migrations-{{ .Schema }}', 'init: ' || (select count(*) from moves_view));

		create index moves_view_idx on moves_view(transactions_id);

		-- disable triggers
		set session_replication_role = replica;

		loop
			with data as (
				select transactions_id, volumes
				from moves_view
				-- play better than offset/limit
				where transactions_id >= _offset and transactions_id < _offset + _batch_size
			)
			update transactions
			set post_commit_volumes = data.volumes
			from data
			where transactions.id = data.transactions_id;

			exit when not found;

			_offset = _offset + _batch_size;

			perform pg_notify('migrations-{{ .Schema }}', 'continue: ' || _batch_size);

			commit;
		end loop;

		-- enable triggers
		set session_replication_role = default;

		drop table if exists moves_view;

 		alter table transactions
 		add constraint post_commit_volumes_not_null
 		check (post_commit_volumes is not null)
 		not valid;
	end
$$;
