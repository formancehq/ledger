set search_path = '{{.Bucket}}';

do $$
begin
	assert (
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
		) v
    ) = 0, 'All accounts volumes should be ok';
end$$;