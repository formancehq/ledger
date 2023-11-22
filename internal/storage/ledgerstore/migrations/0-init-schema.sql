/**
  Some utils
 */
create aggregate aggregate_objects(jsonb) (
  sfunc = jsonb_concat,
  stype = jsonb,
  initcond = '{}'
);

create function first_agg (anyelement, anyelement)
    returns anyelement
    language sql
    immutable
    strict
    parallel safe
as $$
    select $1
$$;

create aggregate first (anyelement) (
    sfunc    = first_agg,
    stype    = anyelement,
    parallel = safe
);

create function array_distinct(anyarray)
    returns anyarray
    language sql
    immutable
as $$
    select array_agg(distinct x)
    from unnest($1) t(x);
$$;

/** Define types **/
create type account_with_volumes as (
    address varchar,
    metadata jsonb,
    volumes jsonb
);

create type volumes as (
    inputs numeric,
    outputs numeric
);

create type volumes_with_asset as (
    asset varchar,
    volumes volumes
);

/** Define tables **/
create table transactions (
    id numeric not null primary key,
    timestamp timestamp without time zone not null,
    reference varchar,
    reverted_at timestamp without time zone,
    postings varchar not null,
    sources jsonb,
    destinations jsonb,
    sources_arrays jsonb,
    destinations_arrays jsonb
);

create table transactions_metadata (
    transaction_id numeric not null references transactions(id),
    revision numeric default 0 not null,
    date timestamp not null,
    metadata jsonb not null default '{}'::jsonb,

    primary key (transaction_id, revision)
);

create table accounts (
    address varchar primary key,
    address_array jsonb not null,
    insertion_date timestamp not null
);

create table accounts_metadata (
    address varchar references accounts(address),
    metadata jsonb default '{}'::jsonb,
    revision numeric default 0,
    date timestamp
);

create table moves (
    seq serial not null primary key ,
    transaction_id numeric not null references transactions(id),
    account_address varchar not null,
    account_address_array jsonb not null,
    asset varchar not null,
    amount numeric not null,
    insertion_date timestamp not null,
    effective_date timestamp not null,
    post_commit_volumes volumes not null,
    post_commit_effective_volumes volumes default null,
    is_source boolean not null
);

create type log_type as enum (
    'NEW_TRANSACTION',
    'REVERTED_TRANSACTION',
    'SET_METADATA',
    'DELETE_METADATA'
);

create table logs (
    id numeric not null primary key,
    type log_type not null,
    hash bytea not null,
    date timestamp not null,
    data jsonb not null,
    idempotency_key varchar(255)
);

/** Define index **/

create function balance_from_volumes(v volumes)
    returns numeric
    language sql
    immutable
as $$
    select v.inputs - v.outputs
$$;

/** Index required for write part */
create index moves_range_dates on moves (account_address, asset, effective_date);

/** Index requires for read */
create index transactions_date on transactions (timestamp);
create index transactions_metadata_metadata on transactions_metadata using gin (metadata);
--create unique index transactions_revisions on transactions_metadata(id desc, revision desc);
create index transactions_sources on transactions using gin (sources jsonb_path_ops);
create index transactions_destinations on transactions using gin (destinations jsonb_path_ops);
create index transactions_sources_arrays on transactions using gin (sources_arrays jsonb_path_ops);
create index transactions_destinations_arrays on transactions using gin (destinations_arrays jsonb_path_ops);

create index moves_account_address on moves (account_address);
create index moves_account_address_array on moves using gin (account_address_array jsonb_ops);
create index moves_account_address_array_length on moves (jsonb_array_length(account_address_array));
create index moves_date on moves (effective_date);
create index moves_asset on moves (asset);
create index moves_balance on moves (balance_from_volumes(post_commit_volumes));
create index moves_post_commit_volumes on moves(account_address, asset, seq);
create index moves_effective_post_commit_volumes on moves(account_address, asset, effective_date desc, seq desc);
create index moves_transactions_id on moves (transaction_id);

create index accounts_address_array on accounts using gin (address_array jsonb_ops);
create index accounts_address_array_length on accounts (jsonb_array_length(address_array));

create unique index accounts_metadata_revisions on accounts_metadata(address asc, revision desc);

/** Define write functions **/
create function insert_new_account(_address varchar, _date timestamp)
    returns bool
    language plpgsql
as $$
    declare
        _account accounts;
    begin
        insert into accounts(address, address_array, insertion_date)
        values (_address, to_json(string_to_array(_address, ':')), _date)
        on conflict do nothing
        returning * into _account;

        return _account is not null;
    end;
$$;

-- given the input : "a:b:c", the function will produce : '{"0": "a", "1": "b", "2": "c", "3": null}'
create function explode_address(_address varchar)
	returns jsonb
	language sql
	immutable
as $$
    select aggregate_objects(jsonb_build_object(data.number - 1, data.value))
	from (
	    select row_number() over () as number, v.value
	    from (
	        select unnest(string_to_array(_address, ':')) as value
	        union all
	        select null
	    ) v
    ) data
$$;

create function get_account(_account_address varchar, _before timestamp default null)
    returns setof accounts_metadata
    language sql
    stable
as $$
    select distinct on (address) *
    from accounts_metadata t
    where (_before is null or t.date <= _before)
        and t.address = _account_address
    order by address, revision desc
    limit 1;
$$;

create function get_transaction(_id numeric, _before timestamp default null)
    returns setof transactions
    language sql
    stable
as $$
    select *
    from transactions t
    where (_before is null or t.timestamp <= _before) and t.id = _id
    order by id desc
    limit 1;
$$;

-- a simple 'select distinct asset from moves' would be more simple
-- but Postgres is extremely inefficient with distinct
-- so the query implementation use a "hack" to emulate skip scan feature which Postgres lack natively
-- see https://wiki.postgresql.org/wiki/Loose_indexscan for more information
create function get_all_assets()
    returns setof varchar
    language sql
as $$
    with recursive t as (
        select min(asset) as asset
        from moves
        union all
        select (
            select min(asset)
            from moves
            where asset > t.asset
        )
        from t
        where t.asset is not null
    )
    select asset from t where asset is not null
    union all
    select null where exists(select 1 from moves where asset is null)
$$;

create function get_latest_move_for_account_and_asset(_account_address varchar, _asset varchar, _before timestamp default null)
    returns setof moves
    language sql
    stable
as $$
    select *
    from moves s
    where (_before is null or s.effective_date <= _before) and s.account_address = _account_address and s.asset = _asset
    order by effective_date desc, seq desc
    limit 1;
$$;

create function update_account_metadata(_address varchar, _metadata jsonb, _date timestamp)
    returns void
    language sql
as $$
    select insert_new_account(_address, _date);

    insert into accounts_metadata (address, metadata, date, revision)
    (
        select _address, accounts_metadata.metadata || _metadata, _date, accounts_metadata.revision + 1
        from accounts_metadata
        where address = _address
        order by revision desc
        limit 1
    )
    union all -- if no metdata
    select _address, _metadata, _date, 0
    limit 1;
$$;

create function delete_account_metadata(_address varchar, _key varchar, _date timestamp)
    returns void
    language sql
as $$
    insert into accounts_metadata (address, metadata, date, revision)
    select _address, accounts_metadata.metadata - _key, _date, accounts_metadata.revision + 1
    from accounts_metadata
    where address = _address
    order by revision desc
    limit 1
$$;

create function update_transaction_metadata(_id numeric, _metadata jsonb, _date timestamp)
    returns void
    language sql
as $$
    insert into transactions_metadata (transaction_id, metadata, date, revision)
    (
        select originalTX.transaction_id,
               originalTX.metadata || _metadata,
               _date,
                originalTX.revision + 1
        from transactions_metadata originalTX
        where transaction_id = _id
        order by revision desc
        limit 1
    )
    union all (
        select _id, '{}'::jsonb, null, -1
    )
    limit 1
$$;

create function delete_transaction_metadata(_id numeric, _key varchar, _date timestamp)
    returns void
    language sql
as $$
    insert into transactions_metadata (transaction_id, metadata, date, revision)
    select originalTX.transaction_id,
           originalTX.metadata - _key,
           _date,
           originalTX.revision + 1
    from transactions_metadata originalTX
    where transaction_id = _id
    order by revision desc
    limit 1;
$$;

create function revert_transaction(_id numeric, _date timestamp)
    returns void
    language sql
as $$
    update transactions
    set reverted_at = _date
    where id = _id;
$$;

create or replace function insert_move(_transaction_id numeric, _insertion_date timestamp without time zone,
    _effective_date timestamp without time zone, _account_address varchar, _asset varchar, _amount numeric, _is_source bool, _new_account bool)
    returns void
    language plpgsql
as $$
    declare
        _post_commit_volumes volumes = (0, 0)::volumes;
        _effective_post_commit_volumes volumes = (0, 0)::volumes;
        _seq numeric;
    begin

        -- todo: lock if we enable parallelism
        -- perform *
        -- from accounts
        -- where address = _account_address
        -- for update;

        if not _new_account then
            select (post_commit_volumes).inputs, (post_commit_volumes).outputs into _post_commit_volumes
            from moves
            where account_address = _account_address
                and asset = _asset
            order by seq desc
            limit 1;

            if not found then
                _post_commit_volumes = (0, 0)::volumes;
                _effective_post_commit_volumes = (0, 0)::volumes;
            else
                select (post_commit_effective_volumes).inputs, (post_commit_effective_volumes).outputs into _effective_post_commit_volumes
                from moves
                where account_address = _account_address
                    and asset = _asset and effective_date <= _effective_date
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

        insert into moves (
            insertion_date,
            effective_date,
            account_address,
            asset,
            transaction_id,
            amount,
            is_source,
            account_address_array,
            post_commit_volumes,
            post_commit_effective_volumes
        ) values (_insertion_date, _effective_date, _account_address, _asset, _transaction_id,
                  _amount, _is_source, (select to_json(string_to_array(_account_address, ':'))),
                  _post_commit_volumes, _effective_post_commit_volumes)
        returning seq into _seq;

        if not _new_account then
            update moves
            set post_commit_effective_volumes = (
                (post_commit_effective_volumes).inputs + case when _is_source then 0 else _amount end,
                (post_commit_effective_volumes).outputs + case when _is_source then _amount else 0 end
            )
            where account_address = _account_address and asset = _asset and effective_date > _effective_date;

            update moves
            set post_commit_effective_volumes = (
                (post_commit_effective_volumes).inputs + case when _is_source then 0 else _amount end,
                (post_commit_effective_volumes).outputs + case when _is_source then _amount else 0 end
            )
            where account_address = _account_address and asset = _asset and effective_date = _effective_date and seq > _seq;
        end if;
    end;
$$;

create function insert_posting(_transaction_id numeric, _insertion_date timestamp without time zone, _effective_date timestamp without time zone, posting jsonb)
    returns void
    language plpgsql
as $$
    declare
        source_created bool;
        destination_created bool;
    begin
        select insert_new_account(posting->>'source', _insertion_date) into source_created;
        select insert_new_account(posting->>'destination', _insertion_date) into destination_created;

        -- todo: sometimes the balance is known at commit time (for sources != world), we need to forward the value to populate the pre_commit_aggregated_input and output
        perform insert_move(_transaction_id, _insertion_date, _effective_date,
            posting->>'source', posting->>'asset', (posting->>'amount')::numeric, true, source_created);
        perform insert_move(_transaction_id, _insertion_date, _effective_date,
            posting->>'destination', posting->>'asset', (posting->>'amount')::numeric, false, destination_created);
    end;
$$;

-- todo: maybe we could avoid plpgsql functions
create function insert_transaction(data jsonb, _date timestamp without time zone)
    returns void
    language plpgsql
as $$
    declare
        posting jsonb;
    begin
        insert into transactions (id, timestamp, reference, postings, sources, destinations, sources_arrays, destinations_arrays)
        values ((data->>'id')::numeric,
                (data->>'timestamp')::timestamp without time zone,
                data->>'reference',
                jsonb_pretty(data->'postings'),
                (
	                select to_jsonb(array_agg(v->>'source')) as value
	                from jsonb_array_elements(data->'postings') v
                ),
                (
	                select to_jsonb(array_agg(v->>'destination')) as value
	                from jsonb_array_elements(data->'postings') v
                ),
                (
	                select to_jsonb(array_agg(explode_address(v->>'source'))) as value
	                from jsonb_array_elements(data->'postings') v
                ),
                (
	                select to_jsonb(array_agg(explode_address(v->>'destination'))) as value
	                from jsonb_array_elements(data->'postings') v
                )
        );

        for posting in (select jsonb_array_elements(data->'postings')) loop
            -- todo: sometimes the balance is known at commit time (for sources != world), we need to forward the value to populate the pre_commit_aggregated_input and output
            perform insert_posting((data->>'id')::numeric, _date, (data->>'timestamp')::timestamp without time zone, posting);
        end loop;

        if data->'metadata' is not null and data->>'metadata' <> '()' then
            insert into transactions_metadata (transaction_id, revision, date, metadata) values (
                (data->>'id')::numeric,
                0,
                (data->>'timestamp')::timestamp without time zone,
                coalesce(data->'metadata', '{}'::jsonb)
            );
        end if;
    end
$$;

create function handle_log() returns trigger
  security definer
  language plpgsql
as $$
  declare
    _key varchar;
    _value jsonb;
  begin
    if new.type = 'NEW_TRANSACTION' then
      perform insert_transaction(new.data->'transaction', new.date);
      for _key, _value in (select * from jsonb_each_text(new.data->'accountMetadata')) loop
          perform update_account_metadata(_key, _value, (new.data->'transaction'->>'timestamp')::timestamp);
      end loop;
    end if;
    if new.type = 'REVERTED_TRANSACTION' then
        perform insert_transaction(new.data->'transaction', new.date);
        perform revert_transaction((new.data->>'revertedTransactionID')::numeric, (new.data->'transaction'->>'timestamp')::timestamp);
    end if;
    if new.type = 'SET_METADATA' then
        if new.data->>'targetType' = 'TRANSACTION' then
            perform update_transaction_metadata((new.data->>'targetId')::numeric, new.data->'metadata', new.date);
        else
            perform update_account_metadata((new.data->>'targetId')::varchar, new.data ->'metadata', new.date);
        end if;
    end if;
    if new.type = 'DELETE_METADATA' then
        if new.data->>'targetType' = 'TRANSACTION' then
            perform delete_transaction_metadata((new.data->>'targetId')::numeric, new.data->>'key', new.date);
        else
            perform delete_account_metadata((new.data->>'targetId')::varchar, new.data ->>'key', new.date);
        end if;
    end if;

    return new;
  end;
$$;

/** Define the trigger which populate table in response to new logs **/
create trigger insert_log after insert on logs
    for each row execute procedure handle_log();

create or replace function get_all_account_effective_volumes(_account varchar, _before timestamp default null)
    returns setof volumes_with_asset
    language sql
    stable
as $$
    with
        all_assets as (
            select v.v as asset
            from get_all_assets() v
        ),
        moves as (
            select m.*
            from all_assets assets
            join lateral (
                select *
                from moves s
                where (_before is null or s.effective_date <= _before) and s.account_address = _account and s.asset = assets.asset
                order by effective_date desc, seq desc
                limit 1
            ) m on true
        )
    select moves.asset, moves.post_commit_effective_volumes
    from moves
$$;

create or replace function get_all_account_volumes(_account varchar, _before timestamp default null)
    returns setof volumes_with_asset
    language sql
    stable
as $$
    with
        all_assets as (
            select v.v as asset
            from get_all_assets() v
        ),
        moves as (
            select m.*
            from all_assets assets
            join lateral (
                select *
                from moves s
                where (_before is null or s.insertion_date <= _before) and s.account_address = _account and s.asset = assets.asset
                order by seq desc
                limit 1
            ) m on true
        )
    select moves.asset, moves.post_commit_volumes
    from moves
$$;

create function volumes_to_jsonb(v volumes_with_asset)
    returns jsonb
    language sql
    immutable
as $$
    select ('{"' || v.asset || '": {"input": ' || (v.volumes).inputs || ', "output": ' || (v.volumes).outputs || '}}')::jsonb
$$;

create function get_account_aggregated_effective_volumes(_account_address varchar, _before timestamp default null)
    returns jsonb
    language sql
    stable
as $$
    select aggregate_objects(volumes_to_jsonb(volumes_with_asset))
    from get_all_account_effective_volumes(_account_address, _before := _before) volumes_with_asset
$$;

create function get_account_aggregated_volumes(_account_address varchar, _before timestamp default null)
    returns jsonb
    language sql
    stable
    parallel safe
as $$
    select aggregate_objects(volumes_to_jsonb(volumes_with_asset))
    from get_all_account_volumes(_account_address, _before := _before) volumes_with_asset
$$;

create function get_account_balance(_account varchar, _asset varchar, _before timestamp default null)
    returns numeric
    language sql
    stable
as $$
    select (post_commit_volumes).inputs - (post_commit_volumes).outputs
    from moves s
    where (_before is null or s.effective_date <= _before) and s.account_address = _account and s.asset = _asset
    order by seq desc
    limit 1
$$;

create function aggregate_ledger_volumes(
    _before timestamp default null,
    _accounts varchar[] default null,
    _assets varchar[] default null
)
    returns setof volumes_with_asset
    language sql
    stable
as $$
    with
        moves as (
            select distinct on (m.account_address, m.asset) m.*
            from moves m
            where (_before is null or m.effective_date <= _before) and
                (_accounts is null or account_address = any(_accounts)) and
                (_assets is null or asset = any(_assets))
            order by account_address, asset, m.seq desc
        )
    select v.asset, (sum((v.post_commit_effective_volumes).inputs), sum((v.post_commit_effective_volumes).outputs))
    from moves v
    group by v.asset
$$;

create function get_aggregated_effective_volumes_for_transaction(tx transactions) returns jsonb
    stable
    language sql
as
$$
select aggregate_objects(jsonb_build_object(data.account_address, data.aggregated))
from (
    select distinct on (move.account_address, move.asset) move.account_address,
        volumes_to_jsonb((move.asset, first(move.post_commit_effective_volumes))) as aggregated
    from moves move
    where move.transaction_id = tx.id
    group by move.account_address, move.asset
) data
$$;

create function get_aggregated_volumes_for_transaction(tx transactions) returns jsonb
    stable
    language sql
as
$$
select aggregate_objects(jsonb_build_object(data.account_address, data.aggregated))
from (
    select distinct on (move.account_address, move.asset) move.account_address,
        volumes_to_jsonb((move.asset, first(move.post_commit_volumes))) as aggregated
    from moves move
    where move.transaction_id = tx.id
    group by move.account_address, move.asset
) data
$$;
