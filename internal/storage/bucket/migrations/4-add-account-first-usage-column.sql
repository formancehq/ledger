alter table "{{.Bucket}}".accounts
add column first_usage timestamp without time zone;

create or replace function "{{.Bucket}}".insert_move(
    _transactions_seq bigint,
    _ledger varchar,
    _insertion_date timestamp without time zone,
    _effective_date timestamp without time zone,
    _account_address varchar,
    _asset varchar,
    _amount numeric,
    _is_source bool,
    _account_exists bool)
    returns void
    language plpgsql
as
$$
declare
    _post_commit_volumes           "{{.Bucket}}".volumes = (0, 0)::"{{.Bucket}}".volumes;
    _effective_post_commit_volumes "{{.Bucket}}".volumes = (0, 0)::"{{.Bucket}}".volumes;
    _seq                           bigint;
    _account_seq                   bigint;
begin

    select seq from "{{.Bucket}}".accounts where ledger = _ledger and address = _account_address into _account_seq;

    if _account_exists then
        select (post_commit_volumes).inputs, (post_commit_volumes).outputs
        into _post_commit_volumes
        from "{{.Bucket}}".moves
        where accounts_seq = _account_seq
          and asset = _asset
        order by seq desc
        limit 1;

        if not found then
            _post_commit_volumes = (0, 0)::"{{.Bucket}}".volumes;
            _effective_post_commit_volumes = (0, 0)::"{{.Bucket}}".volumes;
        else
            select (post_commit_effective_volumes).inputs, (post_commit_effective_volumes).outputs into _effective_post_commit_volumes
            from "{{.Bucket}}".moves
            where accounts_seq = _account_seq
              and asset = _asset
              and effective_date <= _effective_date
            order by effective_date desc, seq desc
            limit 1;

            if not found then
                _effective_post_commit_volumes = (0, 0)::"{{.Bucket}}".volumes;
            end if;
        end if;
    end if;

    if _is_source then
        _post_commit_volumes.outputs = _post_commit_volumes.outputs + _amount;
        _effective_post_commit_volumes.outputs = _effective_post_commit_volumes.outputs + _amount;
    else
        _post_commit_volumes.inputs = _post_commit_volumes.inputs + _amount;
        _effective_post_commit_volumes.inputs = _effective_post_commit_volumes.inputs + _amount;
    end if;

    insert into "{{.Bucket}}".moves (ledger,
                       insertion_date,
                       effective_date,
                       accounts_seq,
                       account_address,
                       asset,
                       transactions_seq,
                       amount,
                       is_source,
                       account_address_array,
                       post_commit_volumes,
                       post_commit_effective_volumes)
    values (_ledger,
            _insertion_date,
            _effective_date,
            _account_seq,
            _account_address,
            _asset,
            _transactions_seq,
            _amount,
            _is_source,
            (select to_json(string_to_array(_account_address, ':'))),
            _post_commit_volumes,
            _effective_post_commit_volumes)
    returning seq into _seq;

    if _account_exists then
        update "{{.Bucket}}".moves
        set post_commit_effective_volumes =
                ((post_commit_effective_volumes).inputs + case when _is_source then 0 else _amount end,
                 (post_commit_effective_volumes).outputs + case when _is_source then _amount else 0 end
                    )
        where accounts_seq = _account_seq
          and asset = _asset
          and effective_date > _effective_date;
    end if;
end;
$$;

create or replace function "{{.Bucket}}".upsert_account(_ledger varchar, _address varchar, _metadata jsonb, _date timestamp, _first_usage timestamp)
    returns void
    language plpgsql
as
$$
begin
    insert into "{{.Bucket}}".accounts(ledger, address, address_array, insertion_date, metadata, updated_at, first_usage)
    values (_ledger, _address, to_json(string_to_array(_address, ':')), _date, coalesce(_metadata, '{}'::jsonb), _date, _first_usage)
    on conflict (ledger, address) do update
        set metadata   = accounts.metadata || coalesce(_metadata, '{}'::jsonb),
            updated_at = _date,
            first_usage = case when accounts.first_usage < _first_usage then accounts.first_usage else _first_usage end
    where not accounts.metadata @> coalesce(_metadata, '{}'::jsonb) or accounts.first_usage > _first_usage;
end;
$$;

create or replace function "{{.Bucket}}".insert_posting(_transaction_seq bigint, _ledger varchar, _insertion_date timestamp without time zone,
                               _effective_date timestamp without time zone, posting jsonb, _account_metadata jsonb)
    returns void
    language plpgsql
as
$$
declare
    _source_exists      bool;
    _destination_exists bool;
begin

    select true from "{{.Bucket}}".accounts where ledger = _ledger and address = posting ->> 'source' into _source_exists;
    perform "{{.Bucket}}".upsert_account(_ledger, posting ->> 'source', _account_metadata -> (posting ->> 'source'), _insertion_date, _effective_date);

    select true from "{{.Bucket}}".accounts where ledger = _ledger and address = posting ->> 'destination' into _destination_exists;
    perform "{{.Bucket}}".upsert_account(_ledger, posting ->> 'destination', _account_metadata -> (posting ->> 'destination'), _insertion_date, _effective_date);

    perform "{{.Bucket}}".insert_move(_transaction_seq, _ledger, _insertion_date, _effective_date,
                        posting ->> 'source', posting ->> 'asset', (posting ->> 'amount')::numeric, true,
                        _source_exists);
    perform "{{.Bucket}}".insert_move(_transaction_seq, _ledger, _insertion_date, _effective_date,
                        posting ->> 'destination', posting ->> 'asset', (posting ->> 'amount')::numeric, false,
                        _destination_exists);
end;
$$;

create or replace function "{{.Bucket}}".handle_log() returns trigger
    security definer
    language plpgsql
as
$$
declare
    _key   varchar;
    _value jsonb;
begin
    if new.type = 'NEW_TRANSACTION' then
        perform "{{.Bucket}}".insert_transaction(new.ledger, new.data -> 'transaction', new.date, new.data -> 'accountMetadata');
        for _key, _value in (select * from jsonb_each_text(new.data -> 'accountMetadata'))
            loop
                perform "{{.Bucket}}".upsert_account(new.ledger, _key, _value,
                                       (new.data -> 'transaction' ->> 'timestamp')::timestamp,
                                       (new.data -> 'transaction' ->> 'timestamp')::timestamp);
            end loop;
    end if;
    if new.type = 'REVERTED_TRANSACTION' then
        perform "{{.Bucket}}".insert_transaction(new.ledger, new.data -> 'transaction', new.date, '{}'::jsonb);
        perform "{{.Bucket}}".revert_transaction(new.ledger, (new.data ->> 'revertedTransactionID')::numeric,
                                   (new.data -> 'transaction' ->> 'timestamp')::timestamp);
    end if;
    if new.type = 'SET_METADATA' then
        if new.data ->> 'targetType' = 'TRANSACTION' then
            perform "{{.Bucket}}".update_transaction_metadata(new.ledger, (new.data ->> 'targetId')::numeric, new.data -> 'metadata',
                                                new.date);
        else
            perform "{{.Bucket}}".upsert_account(new.ledger, (new.data ->> 'targetId')::varchar, new.data -> 'metadata', new.date, new.date);
        end if;
    end if;
    if new.type = 'DELETE_METADATA' then
        if new.data ->> 'targetType' = 'TRANSACTION' then
            perform "{{.Bucket}}".delete_transaction_metadata(new.ledger, (new.data ->> 'targetId')::numeric, new.data ->> 'key',
                                                new.date);
        else
            perform "{{.Bucket}}".delete_account_metadata(new.ledger, (new.data ->> 'targetId')::varchar, new.data ->> 'key',
                                            new.date);
        end if;
    end if;

    return new;
end;
$$;

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
                   where (_before is null or s.insertion_date <= _before)
                     and s.account_address = _account
                     and s.asset = assets.asset
                     and s.ledger = _ledger
                   order by seq desc
                   limit 1
                   ) m on true)
select moves.asset, moves.post_commit_volumes
from moves
$$;

drop function "{{.Bucket}}".upsert_account(_ledger varchar, _address varchar, _metadata jsonb, _date timestamp);

create index accounts_first_usage on "{{.Bucket}}".accounts (first_usage);

update "{{.Bucket}}".accounts
set first_usage = (
    select min(effective_date)
    from "{{.Bucket}}".moves m
    where m.accounts_seq = accounts.seq
    union all
    select accounts.insertion_date
    limit 1
)
where first_usage is null;