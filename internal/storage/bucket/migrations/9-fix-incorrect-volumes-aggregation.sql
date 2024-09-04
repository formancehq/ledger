create or replace function "{{.Bucket}}".get_aggregated_volumes_for_transaction(_ledger varchar, tx numeric) returns jsonb
    stable
    language sql
as
$$
select "{{.Bucket}}".aggregate_objects(jsonb_build_object(data.account_address, data.aggregated))
from (
    select distinct on (move.account_address, move.asset)
        move.account_address,
        "{{.Bucket}}".volumes_to_jsonb((move.asset, "{{.Bucket}}".first(move.post_commit_volumes))) as aggregated
    from (select * from "{{.Bucket}}".moves order by seq desc) move
    where move.transactions_seq = tx and
          ledger = _ledger
      group by move.account_address, move.asset
) data
$$;

create or replace function "{{.Bucket}}".get_aggregated_effective_volumes_for_transaction(_ledger varchar, tx numeric) returns jsonb
    stable
    language sql
as
$$
select "{{.Bucket}}".aggregate_objects(jsonb_build_object(data.account_address, data.aggregated))
from (
    select distinct on (move.account_address, move.asset)
        move.account_address,
        "{{.Bucket}}".volumes_to_jsonb((move.asset, "{{.Bucket}}".first(move.post_commit_effective_volumes))) as aggregated
    from (select * from "{{.Bucket}}".moves order by seq desc) move
    where move.transactions_seq = tx
        and ledger = _ledger
    group by move.account_address, move.asset
) data
$$;