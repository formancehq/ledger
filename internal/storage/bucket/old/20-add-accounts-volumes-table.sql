create table accounts_volumes (
    ledger varchar not null,
    accounts_address varchar not null,
    asset varchar not null,
	input numeric not null,
	output numeric not null,

    primary key (ledger, accounts_address, asset)
);

--todo: handle conflicts while we are inserting and the ledger is actively writing to the database
insert into accounts_volumes (ledger, accounts_address, asset, input, output)
select distinct on (ledger, accounts_address, asset)
	ledger,
	accounts_address,
	asset,
	(moves.post_commit_volumes).inputs as input,
	(moves.post_commit_volumes).outputs as output
from (
	select distinct (ledger, accounts_address, asset)
		ledger,
		accounts_address,
		asset,
		first_value(post_commit_volumes) over (partition by (accounts_address, asset) order by seq desc) as post_commit_volumes
	from moves
) moves;