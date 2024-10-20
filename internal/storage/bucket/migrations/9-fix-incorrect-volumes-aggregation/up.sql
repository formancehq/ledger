set search_path = '{{.Bucket}}';

create or replace function get_aggregated_volumes_for_transaction(_ledger varchar, tx numeric) returns jsonb
    stable
    language sql
as
$$
select aggregate_objects(jsonb_build_object(data.account_address, data.aggregated))
from (
    select distinct on (move.account_address, move.asset)
        move.account_address,
        volumes_to_jsonb((move.asset, first(move.post_commit_volumes))) as aggregated
    from (select * from moves order by seq desc) move
    where move.transactions_seq = tx and
          ledger = _ledger
      group by move.account_address, move.asset
) data
$$ set search_path from current;

create or replace function get_aggregated_effective_volumes_for_transaction(_ledger varchar, tx numeric) returns jsonb
    stable
    language sql
as
$$
select aggregate_objects(jsonb_build_object(data.account_address, data.aggregated))
from (
    select distinct on (move.account_address, move.asset)
        move.account_address,
        volumes_to_jsonb((move.asset, first(move.post_commit_effective_volumes))) as aggregated
    from (select * from moves order by seq desc) move
    where move.transactions_seq = tx
        and ledger = _ledger
    group by move.account_address, move.asset
) data
$$ set search_path from current;