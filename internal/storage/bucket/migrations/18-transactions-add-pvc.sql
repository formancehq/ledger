-- add post_commit_volumes column on transactions table
alter table "{{.Bucket}}".transactions
add column post_commit_volumes jsonb;

update "{{.Bucket}}".transactions
set
	post_commit_volumes = (
		select public.aggregate_objects(post_commit_volumes::jsonb) as post_commit_volumes
		from (
			select accounts_address, json_build_object(accounts_address, post_commit_volumes) post_commit_volumes
			from (
				select accounts_address, json_build_object(asset, post_commit_volumes) as post_commit_volumes
				from (
					select distinct on (accounts_address, asset) accounts_address, asset, first_value(post_commit_volumes) over (partition by accounts_address, asset order by seq desc) as post_commit_volumes
					from "{{.Bucket}}".moves
				) moves
			) values
		) values
		where transactions.sources ? accounts_address or transactions.destinations ? accounts_address
	);

alter table "{{.Bucket}}".transactions
alter column post_commit_volumes set not null ;