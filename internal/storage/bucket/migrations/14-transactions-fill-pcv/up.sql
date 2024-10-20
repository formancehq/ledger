set search_path = '{{.Bucket}}';

do $$
	declare
		_batch_size integer := 30;
	begin
		loop
			with _outdated_transactions as (
				select id
				from transactions
				where post_commit_volumes is null
				limit _batch_size
			)
			update transactions
			set post_commit_volumes = (
				select public.aggregate_objects(post_commit_volumes::jsonb) as post_commit_volumes
				from (
					select accounts_address, json_build_object(accounts_address, post_commit_volumes) post_commit_volumes
					from (
						select accounts_address, json_build_object(asset, post_commit_volumes) as post_commit_volumes
						from (
							select distinct on (accounts_address, asset)
								accounts_address,
								asset,
										first_value(post_commit_volumes) over (
									partition by accounts_address, asset
									order by seq desc
									) as post_commit_volumes
							from moves
							where transactions_id = transactions.id and ledger = transactions.ledger
						) moves
					) values
				) values
			)
			from _outdated_transactions
			where transactions.id in (_outdated_transactions.id);

			exit when not found;
		end loop;
	end
$$;

alter table transactions
alter column post_commit_volumes set not null ;