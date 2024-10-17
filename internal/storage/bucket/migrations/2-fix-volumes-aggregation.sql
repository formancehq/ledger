create or replace function "{{.Bucket}}".get_all_account_volumes(_ledger varchar, _account varchar, _before timestamp default null)
    returns setof "{{.Bucket}}".volumes_with_asset
    language sql
    stable
as
$$
with all_assets as (select v.v as asset
                    from "{{.Bucket}}".get_all_assets(_ledger) v),
     moves as (select m.*
               from all_assets assets
                        join lateral (
                   select *
                   from "{{.Bucket}}".moves s
                   where (_before is null or s.effective_date <= _before)
                     and s.account_address = _account
                     and s.asset = assets.asset
                     and s.ledger = _ledger
                   order by seq desc
                   limit 1
                   ) m on true)
select moves.asset, moves.post_commit_volumes
from moves
$$;