do $$
	declare
		_missing record;
		_count integer;
	begin
		set search_path = '{{.Schema}}';

		select count(*)
		from (
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
			)
		) data
		into _count;

		perform pg_notify('migrations-{{ .Schema }}', 'init: ' || _count);

		loop
			select distinct on (ledger, accounts_address, asset)
				ledger,
				accounts_address,
				asset,
				first_value(post_commit_volumes) over (
					partition by ledger, accounts_address, asset
					order by seq desc
				) as post_commit_volumes
			into _missing
			from moves
			where not exists(
				select
				from accounts_volumes
				where ledger = moves.ledger
					and asset = moves.asset
					and accounts_address = moves.accounts_address
			)
			limit 1;

			exit when not found;

			insert into accounts_volumes (ledger, accounts_address, asset, input, output)
			values (
		        _missing.ledger,
	            _missing.accounts_address,
		        _missing.asset,
				(_missing.post_commit_volumes).inputs,
			(_missing.post_commit_volumes).outputs
			)
			on conflict do nothing; -- can be inserted by a concurrent transaction

			perform pg_notify('migrations-{{ .Schema }}', 'continue: 1');

			commit;
		end loop;
	end
$$;