do $$
	declare
		_count integer;
		_batch_size integer := 100;
	begin
		set search_path = '{{.Schema}}';

		create temporary table tmp_volumes as
		select distinct on (ledger, accounts_address, asset)
			ledger,
			accounts_address,
			asset,
			first_value(post_commit_volumes) over (
				partition by ledger, accounts_address, asset
				order by seq desc
			) as post_commit_volumes
		from moves
		where not exists(
			select
			from accounts_volumes
			where ledger = moves.ledger
				and asset = moves.asset
				and accounts_address = moves.accounts_address
		);

		select count(*)
		from tmp_volumes
		into _count;

		perform pg_notify('migrations-{{ .Schema }}', 'init: ' || _count);

		raise info '_count: %', _count;

		for i in 0.._count by _batch_size loop
			with _rows as (
				select *
				from tmp_volumes
				offset i
				limit _batch_size
			)
			insert into accounts_volumes (ledger, accounts_address, asset, input, output)
			select ledger, accounts_address, asset, (post_commit_volumes).inputs, (post_commit_volumes).outputs
			from _rows
			on conflict do nothing; -- can be inserted by a concurrent transaction

			commit;

			perform pg_notify('migrations-{{ .Schema }}', 'continue: ' || _batch_size);

		end loop;

		drop table tmp_volumes;
	end
$$;