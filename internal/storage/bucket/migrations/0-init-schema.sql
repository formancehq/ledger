create aggregate "{{.Bucket}}".aggregate_objects(jsonb) (
    sfunc = jsonb_concat,
    stype = jsonb,
    initcond = '{}'
    );

create function "{{.Bucket}}".first_agg(anyelement, anyelement)
    returns anyelement
    language sql
    immutable
    strict
    parallel safe
as
$$
select $1
$$;

create aggregate "{{.Bucket}}".first (anyelement) (
    sfunc = first_agg,
    stype = anyelement,
    parallel = safe
    );

create function "{{.Bucket}}".array_distinct(anyarray)
    returns anyarray
    language sql
    immutable
as
$$
select array_agg(distinct x)
from unnest($1) t(x);
$$;

/** Define types **/
create type "{{.Bucket}}".account_with_volumes as
(
    address  varchar,
    metadata jsonb,
    volumes  jsonb
);

create type "{{.Bucket}}".volumes as
(
    inputs  numeric,
    outputs numeric
);

create type "{{.Bucket}}".volumes_with_asset as
(
    asset   varchar,
    volumes "{{.Bucket}}".volumes
);

/** Define tables **/
create table "{{.Bucket}}".transactions
(
    seq                 bigserial primary key,
    ledger              varchar                     not null,
    id                  numeric                     not null,
    timestamp           timestamp without time zone not null,
    reference           varchar,
    reverted_at         timestamp without time zone,
    updated_at          timestamp without time zone,
    postings            varchar                     not null,
    sources             jsonb,
    destinations        jsonb,
    sources_arrays      jsonb,
    destinations_arrays jsonb,
    metadata            jsonb                       not null default '{}'::jsonb
);

create unique index transactions_ledger on "{{.Bucket}}".transactions (ledger, id);
create index transactions_date on "{{.Bucket}}".transactions (timestamp);
create index transactions_metadata_index on "{{.Bucket}}".transactions using gin (metadata jsonb_path_ops);
create index transactions_sources on "{{.Bucket}}".transactions using gin (sources jsonb_path_ops);
create index transactions_destinations on "{{.Bucket}}".transactions using gin (destinations jsonb_path_ops);
create index transactions_sources_arrays on "{{.Bucket}}".transactions using gin (sources_arrays jsonb_path_ops);
create index transactions_destinations_arrays on "{{.Bucket}}".transactions using gin (destinations_arrays jsonb_path_ops);

create table "{{.Bucket}}".transactions_metadata
(
    seq              bigserial,
    ledger           varchar   not null,
    transactions_seq bigint references "{{.Bucket}}".transactions (seq),
    revision         numeric            default 0 not null,
    date             timestamp not null,
    metadata         jsonb     not null default '{}'::jsonb,

    primary key (seq)
);

create index transactions_metadata_metadata on "{{.Bucket}}".transactions_metadata using gin (metadata jsonb_path_ops);
create unique index transactions_metadata_ledger on "{{.Bucket}}".transactions_metadata (ledger, transactions_seq, revision);
create index transactions_metadata_revisions on "{{.Bucket}}".transactions_metadata(transactions_seq asc, revision desc) include (metadata, date);

create table "{{.Bucket}}".accounts
(
    seq            bigserial primary key,
    ledger         varchar   not null,
    address        varchar   not null,
    address_array  jsonb     not null,
    insertion_date timestamp not null,
    updated_at     timestamp not null,
    metadata       jsonb     not null default '{}'::jsonb
);

create unique index accounts_ledger on "{{.Bucket}}".accounts (ledger, address) include (seq);
create index accounts_address_array on "{{.Bucket}}".accounts using gin (address_array jsonb_ops);
create index accounts_address_array_length on "{{.Bucket}}".accounts (jsonb_array_length(address_array));

create table "{{.Bucket}}".accounts_metadata
(
    seq          bigserial primary key,
    ledger       varchar not null,
    accounts_seq bigint references "{{.Bucket}}".accounts (seq),
    metadata     jsonb   not null default '{}'::jsonb,
    revision     numeric          default 0,
    date         timestamp
);

create unique index accounts_metadata_ledger on "{{.Bucket}}".accounts_metadata (ledger, accounts_seq, revision);
create index accounts_metadata_metadata on "{{.Bucket}}".accounts_metadata using gin (metadata jsonb_path_ops);
create index accounts_metadata_revisions on "{{.Bucket}}".accounts_metadata(accounts_seq asc, revision desc) include (metadata, date);

create table "{{.Bucket}}".moves
(
    seq                           bigserial    not null primary key,
    ledger                        varchar   not null,
    transactions_seq              bigint   not null references "{{.Bucket}}".transactions (seq),
    accounts_seq                  bigint   not null references "{{.Bucket}}".accounts (seq),
    account_address               varchar   not null,
    account_address_array         jsonb     not null,
    asset                         varchar   not null,
    amount                        numeric   not null,
    insertion_date                timestamp not null,
    effective_date                timestamp not null,
    post_commit_volumes           "{{.Bucket}}".volumes   not null,
    post_commit_effective_volumes "{{.Bucket}}".volumes default null,
    is_source                     boolean   not null
);

create index moves_ledger on "{{.Bucket}}".moves (ledger);
create index moves_range_dates on "{{.Bucket}}".moves (account_address, asset, effective_date);
create index moves_account_address on "{{.Bucket}}".moves (account_address);
create index moves_account_address_array on "{{.Bucket}}".moves using gin (account_address_array jsonb_ops);
create index moves_account_address_array_length on "{{.Bucket}}".moves (jsonb_array_length(account_address_array));
create index moves_date on "{{.Bucket}}".moves (effective_date);
create index moves_asset on "{{.Bucket}}".moves (asset);
create index moves_post_commit_volumes on "{{.Bucket}}".moves (accounts_seq, asset, seq);
create index moves_effective_post_commit_volumes on "{{.Bucket}}".moves (accounts_seq, asset, effective_date desc);

create type "{{.Bucket}}".log_type as enum
    ('NEW_TRANSACTION',
        'REVERTED_TRANSACTION',
        'SET_METADATA',
        'DELETE_METADATA'
        );

create table "{{.Bucket}}".logs
(
    seq             bigserial primary key,
    ledger          varchar   not null,
    id              numeric   not null,
    type            "{{.Bucket}}".log_type  not null,
    hash            bytea     not null,
    date            timestamp not null,
    data            jsonb     not null,
    idempotency_key varchar(255)
);

create unique index logs_ledger on "{{.Bucket}}".logs (ledger, id);

/** Define index **/

create function "{{.Bucket}}".balance_from_volumes(v "{{.Bucket}}".volumes)
    returns numeric
    language sql
    immutable
as
$$
select v.inputs - v.outputs
$$;

/** Define write functions **/

-- given the input : "a:b:c", the function will produce : '{"0": "a", "1": "b", "2": "c", "3": null}'
create function "{{.Bucket}}".explode_address(_address varchar)
    returns jsonb
    language sql
    immutable
as
$$
select "{{.Bucket}}".aggregate_objects(jsonb_build_object(data.number - 1, data.value))
from (select row_number() over () as number, v.value
      from (select unnest(string_to_array(_address, ':')) as value
            union all
            select null) v) data
$$;

create function "{{.Bucket}}".get_transaction(_ledger varchar, _id numeric, _before timestamp default null)
    returns setof "{{.Bucket}}".transactions
    language sql
    stable
as
$$
select *
from "{{.Bucket}}".transactions t
where (_before is null or t.timestamp <= _before)
  and t.id = _id
  and ledger = _ledger
order by id desc
limit 1;
$$;

-- a simple 'select distinct asset from moves' would be more simple
-- but Postgres is extremely inefficient with distinct
-- so the query implementation use a "hack" to emulate skip scan feature which Postgres lack natively
-- see https://wiki.postgresql.org/wiki/Loose_indexscan for more information
create function "{{.Bucket}}".get_all_assets(_ledger varchar)
    returns setof varchar
    language sql
as
$$
with recursive t as (select min(asset) as asset
                     from "{{.Bucket}}".moves
                     where ledger = _ledger
                     union all
                     select (select min(asset)
                             from "{{.Bucket}}".moves
                             where asset > t.asset
                               and ledger = _ledger)
                     from t
                     where t.asset is not null)
select asset
from t
where asset is not null
union all
select null
where exists(select 1 from "{{.Bucket}}".moves where asset is null and ledger = _ledger)
$$;

create function "{{.Bucket}}".get_latest_move_for_account_and_asset(_ledger varchar, _account_address varchar, _asset varchar,
                                                      _before timestamp default null)
    returns setof "{{.Bucket}}".moves
    language sql
    stable
as
$$
select *
from "{{.Bucket}}".moves s
where (_before is null or s.effective_date <= _before)
  and s.account_address = _account_address
  and s.asset = _asset
  and ledger = _ledger
order by effective_date desc, seq desc
limit 1;
$$;

create function "{{.Bucket}}".upsert_account(_ledger varchar, _address varchar, _metadata jsonb, _date timestamp)
    returns void
    language plpgsql
as
$$
begin
    insert into "{{.Bucket}}".accounts(ledger, address, address_array, insertion_date, metadata, updated_at)
    values (_ledger, _address, to_json(string_to_array(_address, ':')), _date, coalesce(_metadata, '{}'::jsonb), _date)
    on conflict (ledger, address) do update
        set metadata   = accounts.metadata || coalesce(_metadata, '{}'::jsonb),
            updated_at = _date
    where not accounts.metadata @> coalesce(_metadata, '{}'::jsonb);
end;
$$;

create function "{{.Bucket}}".delete_account_metadata(_ledger varchar, _address varchar, _key varchar, _date timestamp)
    returns void
    language plpgsql
as
$$
begin
    update "{{.Bucket}}".accounts
    set metadata   = metadata - _key,
        updated_at = _date
    where address = _address
      and ledger = _ledger;
end
$$;

create function "{{.Bucket}}".update_transaction_metadata(_ledger varchar, _id numeric, _metadata jsonb, _date timestamp)
    returns void
    language plpgsql
as
$$
begin
    update "{{.Bucket}}".transactions
    set metadata = metadata || _metadata,
        updated_at = _date
    where id = _id
      and ledger = _ledger;
end;
$$;

create function "{{.Bucket}}".delete_transaction_metadata(_ledger varchar, _id numeric, _key varchar, _date timestamp)
    returns void
    language plpgsql
as
$$
begin
    update "{{.Bucket}}".transactions
    set metadata = metadata - _key,
        updated_at = _date
    where id = _id
      and ledger = _ledger;
end;
$$;

create function "{{.Bucket}}".revert_transaction(_ledger varchar, _id numeric, _date timestamp)
    returns void
    language sql
as
$$
update "{{.Bucket}}".transactions
set reverted_at = _date
where id = _id
  and ledger = _ledger;
$$;


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

        update "{{.Bucket}}".moves
        set post_commit_effective_volumes =
                ((post_commit_effective_volumes).inputs + case when _is_source then 0 else _amount end,
                 (post_commit_effective_volumes).outputs + case when _is_source then _amount else 0 end
                    )
        where accounts_seq = _account_seq
          and asset = _asset
          and effective_date = _effective_date
          and seq > _seq;
    end if;
end;
$$;

create function "{{.Bucket}}".insert_posting(_transaction_seq bigint, _ledger varchar, _insertion_date timestamp without time zone,
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
    select true from "{{.Bucket}}".accounts where ledger = _ledger and address = posting ->> 'destination' into _destination_exists;

    perform "{{.Bucket}}".upsert_account(_ledger, posting ->> 'source', _account_metadata -> (posting ->> 'source'), _insertion_date);
    perform "{{.Bucket}}".upsert_account(_ledger, posting ->> 'destination', _account_metadata -> (posting ->> 'destination'),
                           _insertion_date);

    perform "{{.Bucket}}".insert_move(_transaction_seq, _ledger, _insertion_date, _effective_date,
                        posting ->> 'source', posting ->> 'asset', (posting ->> 'amount')::numeric, true,
                        _source_exists);
    perform "{{.Bucket}}".insert_move(_transaction_seq, _ledger, _insertion_date, _effective_date,
                        posting ->> 'destination', posting ->> 'asset', (posting ->> 'amount')::numeric, false,
                        _destination_exists);
end;
$$;

create function "{{.Bucket}}".insert_transaction(_ledger varchar, data jsonb, _date timestamp without time zone,
                                   _account_metadata jsonb)
    returns void
    language plpgsql
as
$$
declare
    posting jsonb;
    _seq    bigint;
begin
    insert into "{{.Bucket}}".transactions (ledger, id, timestamp, updated_at, reference, postings, sources,
                              destinations, sources_arrays, destinations_arrays, metadata)
    values (_ledger,
            (data ->> 'id')::numeric,
            (data ->> 'timestamp')::timestamp without time zone,
            (data ->> 'timestamp')::timestamp without time zone,
            data ->> 'reference',
            jsonb_pretty(data -> 'postings'),
            (select to_jsonb(array_agg(v ->> 'source')) as value
             from jsonb_array_elements(data -> 'postings') v),
            (select to_jsonb(array_agg(v ->> 'destination')) as value
             from jsonb_array_elements(data -> 'postings') v),
            (select to_jsonb(array_agg("{{.Bucket}}".explode_address(v ->> 'source'))) as value
             from jsonb_array_elements(data -> 'postings') v),
            (select to_jsonb(array_agg("{{.Bucket}}".explode_address(v ->> 'destination'))) as value
             from jsonb_array_elements(data -> 'postings') v),
            coalesce(data -> 'metadata', '{}'::jsonb))
    returning seq into _seq;

    for posting in (select jsonb_array_elements(data -> 'postings'))
        loop
            perform "{{.Bucket}}".insert_posting(_seq, _ledger, _date, (data ->> 'timestamp')::timestamp without time zone, posting,
                                   _account_metadata);
        end loop;

    if data -> 'metadata' is not null and data ->> 'metadata' <> '()' then
        insert into "{{.Bucket}}".transactions_metadata (ledger, transactions_seq, revision, date, metadata)
        values (_ledger,
                _seq,
                0,
                (data ->> 'timestamp')::timestamp without time zone,
                coalesce(data -> 'metadata', '{}'::jsonb));
    end if;
end
$$;

create function "{{.Bucket}}".handle_log() returns trigger
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
            perform "{{.Bucket}}".upsert_account(new.ledger, (new.data ->> 'targetId')::varchar, new.data -> 'metadata', new.date);
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

create function "{{.Bucket}}".update_account_metadata_history() returns trigger
    security definer
    language plpgsql
as
$$
begin
    insert into "{{.Bucket}}".accounts_metadata (ledger, accounts_seq, revision, date, metadata)
    values (new.ledger, new.seq, (
        select revision + 1
		from "{{.Bucket}}".accounts_metadata
		where accounts_metadata.accounts_seq = new.seq
		order by revision desc
		limit 1
	), new.updated_at, new.metadata);

    return new;
end;
$$;

create function "{{.Bucket}}".insert_account_metadata_history() returns trigger
    security definer
    language plpgsql
as
$$
begin
    insert into "{{.Bucket}}".accounts_metadata (ledger, accounts_seq, revision, date, metadata)
    values (new.ledger, new.seq, 1, new.insertion_date, new.metadata);

    return new;
end;
$$;

create function "{{.Bucket}}".update_transaction_metadata_history() returns trigger
    security definer
    language plpgsql
as
$$
begin
    insert into "{{.Bucket}}".transactions_metadata (ledger, transactions_seq, revision, date, metadata)
    values (new.ledger, new.seq, (select revision + 1
                                  from "{{.Bucket}}".transactions_metadata
                                  where transactions_metadata.transactions_seq = new.seq
                                  order by revision desc
                                  limit 1), new.updated_at, new.metadata);

    return new;
end;
$$;

create function "{{.Bucket}}".insert_transaction_metadata_history() returns trigger
    security definer
    language plpgsql
as
$$
begin
    insert into "{{.Bucket}}".transactions_metadata (ledger, transactions_seq, revision, date, metadata)
    values (new.ledger, new.seq, 1, new.timestamp, new.metadata);

    return new;
end;
$$;

create or replace function "{{.Bucket}}".get_all_account_effective_volumes(_ledger varchar, _account varchar, _before timestamp default null)
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
                   order by effective_date desc, seq desc
                   limit 1
                   ) m on true)
select moves.asset, moves.post_commit_effective_volumes
from moves
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

create function "{{.Bucket}}".volumes_to_jsonb(v "{{.Bucket}}".volumes_with_asset)
    returns jsonb
    language sql
    immutable
as
$$
select ('{"' || v.asset || '": {"input": ' || (v.volumes).inputs || ', "output": ' || (v.volumes).outputs || '}}')::jsonb
$$;

create function "{{.Bucket}}".get_account_aggregated_effective_volumes(_ledger varchar, _account_address varchar,
                                                         _before timestamp default null)
    returns jsonb
    language sql
    stable
as
$$
select "{{.Bucket}}".aggregate_objects("{{.Bucket}}".volumes_to_jsonb(volumes_with_asset))
from "{{.Bucket}}".get_all_account_effective_volumes(_ledger, _account_address, _before := _before) volumes_with_asset
$$;

create function "{{.Bucket}}".get_account_aggregated_volumes(_ledger varchar, _account_address varchar,
                                               _before timestamp default null)
    returns jsonb
    language sql
    stable
    parallel safe
as
$$
select "{{.Bucket}}".aggregate_objects("{{.Bucket}}".volumes_to_jsonb(volumes_with_asset))
from "{{.Bucket}}".get_all_account_volumes(_ledger, _account_address, _before := _before) volumes_with_asset
$$;

create function "{{.Bucket}}".get_account_balance(_ledger varchar, _account varchar, _asset varchar, _before timestamp default null)
    returns numeric
    language sql
    stable
as
$$
select (post_commit_volumes).inputs - (post_commit_volumes).outputs
from "{{.Bucket}}".moves s
where (_before is null or s.effective_date <= _before)
  and s.account_address = _account
  and s.asset = _asset
  and s.ledger = _ledger
order by seq desc
limit 1
$$;

create function "{{.Bucket}}".aggregate_ledger_volumes(
    _ledger varchar,
    _before timestamp default null,
    _accounts varchar[] default null,
    _assets varchar[] default null
)
    returns setof "{{.Bucket}}".volumes_with_asset
    language sql
    stable
as
$$
with moves as (select distinct on (m.account_address, m.asset) m.*
               from "{{.Bucket}}".moves m
               where (_before is null or m.effective_date <= _before)
                 and (_accounts is null or account_address = any (_accounts))
                 and (_assets is null or asset = any (_assets))
                 and m.ledger = _ledger
               order by account_address, asset, m.seq desc)
select v.asset,
       (sum((v.post_commit_effective_volumes).inputs), sum((v.post_commit_effective_volumes).outputs))
from moves v
group by v.asset
$$;

create function "{{.Bucket}}".get_aggregated_effective_volumes_for_transaction(_ledger varchar, tx numeric) returns jsonb
    stable
    language sql
as
$$
select "{{.Bucket}}".aggregate_objects(jsonb_build_object(data.account_address, data.aggregated))
from (select distinct on (move.account_address, move.asset) move.account_address,
                                                            "{{.Bucket}}".volumes_to_jsonb((move.asset, "{{.Bucket}}".first(move.post_commit_effective_volumes))) as aggregated
      from "{{.Bucket}}".moves move
      where move.transactions_seq = tx
        and ledger = _ledger
      group by move.account_address, move.asset) data
$$;

create function "{{.Bucket}}".get_aggregated_volumes_for_transaction(_ledger varchar, tx numeric) returns jsonb
    stable
    language sql
as
$$
select "{{.Bucket}}".aggregate_objects(jsonb_build_object(data.account_address, data.aggregated))
from (select distinct on (move.account_address, move.asset) move.account_address,
                                                            "{{.Bucket}}".volumes_to_jsonb((move.asset, "{{.Bucket}}".first(move.post_commit_volumes))) as aggregated
      from moves move
      where move.transactions_seq = tx
        and ledger = _ledger
      group by move.account_address, move.asset) data
$$;

create trigger "insert_log"
after insert
on "{{.Bucket}}"."logs"
for each row
execute procedure "{{.Bucket}}".handle_log();

create trigger "update_account"
after update
on "{{.Bucket}}"."accounts"
for each row
execute procedure "{{.Bucket}}".update_account_metadata_history();

create trigger "insert_account"
after insert
on "{{.Bucket}}"."accounts"
for each row
execute procedure "{{.Bucket}}".insert_account_metadata_history();

create trigger "update_transaction"
after update
on "{{.Bucket}}"."transactions"
for each row
execute procedure "{{.Bucket}}".update_transaction_metadata_history();

create trigger "insert_transaction"
after insert
on "{{.Bucket}}"."transactions"
for each row
execute procedure "{{.Bucket}}".insert_transaction_metadata_history();