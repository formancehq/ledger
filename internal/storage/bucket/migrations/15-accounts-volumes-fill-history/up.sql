set search_path = '{{.Bucket}}';

do $$
	declare
		_missing record;
	begin
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
		end loop;
	end
$$;