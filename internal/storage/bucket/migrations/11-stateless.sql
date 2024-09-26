drop trigger "insert_log" on "{{.Bucket}}".logs;

drop index transactions_reference;
create unique index transactions_reference on "{{.Bucket}}".transactions (ledger, reference);

alter table "{{.Bucket}}".transactions
add column inserted_at timestamp without time zone
default (now() at time zone 'utc');

-- todo: check if still required
alter table "{{.Bucket}}".transactions
alter column timestamp
set default (now() at time zone 'utc');

alter table "{{.Bucket}}".transactions
add column post_commit_volumes jsonb not null;

alter table "{{.Bucket}}".moves
add column post_commit_volumes_jsonb jsonb;

alter table "{{.Bucket}}".moves
add column post_commit_effective_volumes_jsonb jsonb;

alter table "{{.Bucket}}".moves
drop column transactions_seq;

alter table "{{.Bucket}}".moves
drop column accounts_seq;

alter table "{{.Bucket}}".moves
add column transactions_id bigint not null ;

alter table "{{.Bucket}}".moves
rename column account_address to accounts_address;

alter table "{{.Bucket}}".moves
rename column account_address_array to accounts_address_array;

alter table "{{.Bucket}}".moves
drop column post_commit_volumes;

alter table "{{.Bucket}}".moves
drop column post_commit_effective_volumes;

alter table "{{.Bucket}}".moves
drop column accounts_address_array;

alter table "{{.Bucket}}".moves
rename post_commit_volumes_jsonb to post_commit_volumes;

alter table "{{.Bucket}}".moves
rename post_commit_effective_volumes_jsonb to post_commit_effective_volumes;

alter table "{{.Bucket}}".moves
alter column post_commit_volumes
drop not null,
alter column post_commit_effective_volumes
drop not null;

-- todo: need migrate
alter table "{{.Bucket}}".transactions_metadata
drop column transactions_seq;

alter table "{{.Bucket}}".transactions_metadata
add column transactions_id bigint not null;

-- todo: need migrate
alter table "{{.Bucket}}".accounts_metadata
drop column accounts_seq;

alter table "{{.Bucket}}".accounts_metadata
add column accounts_address varchar not null;

alter table "{{.Bucket}}".transactions
alter column id
type bigint;

alter table "{{.Bucket}}".transactions
drop column seq;

alter table "{{.Bucket}}".logs
alter column hash
drop not null;

-- Change from jsonb to json to keep keys order and ensure consistent hashing
-- todo: check if still required
alter table "{{.Bucket}}".logs
alter column data
type json;

--drop index transactions_metadata_ledger;
--drop index transactions_metadata_revisions;

--drop index accounts_metadata_ledger;
--drop index accounts_metadata_revisions;

create unique index accounts_metadata_ledger on "{{.Bucket}}".accounts_metadata (ledger, accounts_address, revision);
create index accounts_metadata_revisions on "{{.Bucket}}".accounts_metadata(accounts_address asc, revision desc) include (metadata, date);

create unique index transactions_metadata_ledger on "{{.Bucket}}".transactions_metadata (ledger, transactions_id, revision);
create index transactions_metadata_revisions on "{{.Bucket}}".transactions_metadata(transactions_id asc, revision desc) include (metadata, date);

-- todo: add migration
-- update "{{.Bucket}}".moves
-- set post_commit_volumes_jsonb = json_build_object(
-- 	'input', ((moves.post_commit_volumes).inputs),
-- 	'output', ((moves.post_commit_volumes).outputs)
-- );
--
-- update "{{.Bucket}}".moves
-- set post_commit_effective_volumes_jsonb = json_build_object(
-- 	'input', ((moves.post_commit_effective_volumes).inputs),
-- 	'output', ((moves.post_commit_effective_volumes).outputs)
-- );

create table "{{.Bucket}}".accounts_volumes (
    ledger varchar not null,
    accounts_address varchar not null,
    asset varchar not null,
	input numeric not null,
	output numeric not null,

    primary key (ledger, accounts_address, asset)
);

create view "{{.Bucket}}".balances as
select ledger, accounts_address, asset, input - output as balance
from "{{.Bucket}}".accounts_volumes;

insert into "{{.Bucket}}".accounts_volumes (ledger, accounts_address, asset, input, output)
select distinct on (ledger, accounts_address, asset)
	ledger,
	accounts_address,
	asset,
	(moves.post_commit_volumes->>'input')::numeric as input,
	(moves.post_commit_volumes->>'output')::numeric as output
from (
	select *
	from "{{.Bucket}}".moves
	order by seq desc
) moves;

--drop index moves_post_commit_volumes;
--drop index moves_effective_post_commit_volumes;

drop trigger "insert_account"  on "{{.Bucket}}".accounts;
drop trigger "update_account"  on "{{.Bucket}}".accounts;
drop trigger "insert_transaction"  on "{{.Bucket}}".transactions;
drop trigger "update_transaction"  on "{{.Bucket}}".transactions;

--drop index moves_account_address_array;
--drop index moves_account_address_array_length;
drop index transactions_sources_arrays;
drop index transactions_destinations_arrays;
drop index accounts_address_array;
drop index accounts_address_array_length;

drop index transactions_sources;
drop index transactions_destinations;

drop aggregate "{{.Bucket}}".aggregate_objects(jsonb);
drop aggregate "{{.Bucket}}".first(anyelement);

drop function "{{.Bucket}}".array_distinct(anyarray);
drop function "{{.Bucket}}".insert_posting(_transaction_seq bigint, _ledger character varying, _insertion_date timestamp without time zone, _effective_date timestamp without time zone, posting jsonb, _account_metadata jsonb);
drop function "{{.Bucket}}".upsert_account(_ledger character varying, _address character varying, _metadata jsonb, _date timestamp without time zone, _first_usage timestamp without time zone);
drop function "{{.Bucket}}".get_latest_move_for_account_and_asset(_ledger character varying, _account_address character varying, _asset character varying, _before timestamp without time zone);
drop function "{{.Bucket}}".update_transaction_metadata(_ledger character varying, _id numeric, _metadata jsonb, _date timestamp without time zone);
drop function "{{.Bucket}}".delete_account_metadata(_ledger character varying, _address character varying, _key character varying, _date timestamp without time zone);
drop function "{{.Bucket}}".delete_transaction_metadata(_ledger character varying, _id numeric, _key character varying, _date timestamp without time zone);
drop function "{{.Bucket}}".balance_from_volumes(v "{{.Bucket}}".volumes);
drop function "{{.Bucket}}".get_all_account_volumes(_ledger character varying, _account character varying, _before timestamp without time zone);
drop function "{{.Bucket}}".first_agg(anyelement, anyelement);
drop function "{{.Bucket}}".volumes_to_jsonb(v "{{.Bucket}}".volumes_with_asset);
drop function "{{.Bucket}}".get_account_aggregated_effective_volumes(_ledger character varying, _account_address character varying, _before timestamp without time zone);
drop function "{{.Bucket}}".handle_log();
drop function "{{.Bucket}}".get_account_aggregated_volumes(_ledger character varying, _account_address character varying, _before timestamp without time zone);
drop function "{{.Bucket}}".get_aggregated_volumes_for_transaction(_ledger character varying, tx numeric);
drop function "{{.Bucket}}".insert_move(_transactions_seq bigint, _ledger character varying, _insertion_date timestamp without time zone, _effective_date timestamp without time zone, _account_address character varying, _asset character varying, _amount numeric, _is_source boolean, _account_exists boolean);
drop function "{{.Bucket}}".get_all_assets(_ledger character varying);
drop function "{{.Bucket}}".insert_transaction(_ledger character varying, data jsonb, _date timestamp without time zone, _account_metadata jsonb);
drop function "{{.Bucket}}".get_all_account_effective_volumes(_ledger character varying, _account character varying, _before timestamp without time zone);
drop function "{{.Bucket}}".get_account_balance(_ledger character varying, _account character varying, _asset character varying, _before timestamp without time zone);
drop function "{{.Bucket}}".get_aggregated_effective_volumes_for_transaction(_ledger character varying, tx numeric);
drop function "{{.Bucket}}".aggregate_ledger_volumes(_ledger character varying, _before timestamp without time zone, _accounts character varying[], _assets character varying[] );
drop function "{{.Bucket}}".get_transaction(_ledger character varying, _id numeric, _before timestamp without time zone);
--drop function "{{.Bucket}}".explode_address(_address character varying);
drop function "{{.Bucket}}".revert_transaction(_ledger character varying, _id numeric, _date timestamp without time zone);

drop type "{{.Bucket}}".volumes_with_asset;
drop type "{{.Bucket}}".volumes;

create function "{{.Bucket}}".set_effective_volumes()
    returns trigger
    security definer
    language plpgsql
as
$$
begin
    new.post_commit_effective_volumes = coalesce((
        select json_build_object(
            'input', (post_commit_effective_volumes->>'input')::numeric + case when new.is_source then 0 else new.amount end,
            'output', (post_commit_effective_volumes->>'output')::numeric + case when new.is_source then new.amount else 0 end
        )
        from "{{.Bucket}}".moves
        where accounts_address = new.accounts_address
            and asset = new.asset
            and ledger = new.ledger
            and (effective_date < new.effective_date or (effective_date = new.effective_date and seq < new.seq))
        order by effective_date desc, seq desc
        limit 1
    ), json_build_object(
        'input', case when new.is_source then 0 else new.amount end,
        'output', case when new.is_source then new.amount else 0 end
    ));

    return new;
end;
$$;

create function "{{.Bucket}}".update_effective_volumes()
    returns trigger
    security definer
    language plpgsql
as
$$
begin
    update "{{.Bucket}}".moves
    set post_commit_effective_volumes = json_build_object(
		'input', (post_commit_effective_volumes->>'input')::numeric + case when new.is_source then 0 else new.amount end,
		'output', (post_commit_effective_volumes->>'output')::numeric + case when new.is_source then new.amount else 0 end
    )
    where accounts_address = new.accounts_address
        and asset = new.asset
        and effective_date > new.effective_date
        and ledger = new.ledger;

    return new;
end;
$$;

create function "{{.Bucket}}".set_log_hash()
	returns trigger
	security definer
	language plpgsql
as
$$
declare
	previousHash bytea;
	marshalledAsJSON varchar;
begin
	select hash into previousHash
	from "{{.Bucket}}".logs
	where ledger = new.ledger
	order by seq desc
	limit 1;

	-- select only fields participating in the hash on the backend and format json representation the same way
	select public.json_compact(json_build_object(
		'type', new.type,
		'data', new.data,
		'date', to_json(new.date::timestamp)#>>'{}' || 'Z',
		'idempotencyKey', coalesce(new.idempotency_key, ''),
		'id', 0,
		'hash', null
	)) into marshalledAsJSON;

	new.hash = (
		select public.digest(
			case
				when previousHash is null
					then marshalledAsJSON::bytea
					else '"' || encode(previousHash::bytea, 'base64')::bytea || E'"\n' || convert_to(marshalledAsJSON, 'LATIN1')::bytea
			end || E'\n', 'sha256'::text
        )
    );

	return new;
end;
$$;


create or replace function "{{.Bucket}}".update_transaction_metadata_history() returns trigger
	security definer
	language plpgsql
as
$$
begin
	insert into "{{.Bucket}}".transactions_metadata (ledger, transactions_id, revision, date, metadata)
	values (new.ledger, new.id, (
		select revision + 1
		from "{{.Bucket}}".transactions_metadata
		where transactions_metadata.transactions_id = new.id and transactions_metadata.ledger = new.ledger
		order by revision desc
		limit 1
	), new.updated_at, new.metadata);

	return new;
end;
$$;

create or replace function "{{.Bucket}}".insert_transaction_metadata_history() returns trigger
	security definer
	language plpgsql
as
$$
begin
	insert into "{{.Bucket}}".transactions_metadata (ledger, transactions_id, revision, date, metadata)
	values (new.ledger, new.id, 1, new.timestamp, new.metadata);

	return new;
end;
$$;

create or replace function "{{.Bucket}}".update_account_metadata_history() returns trigger
	security definer
	language plpgsql
as
$$
begin
	insert into "{{.Bucket}}".accounts_metadata (ledger, accounts_address, revision, date, metadata)
	values (new.ledger, new.address, (
		select revision + 1
		from "{{.Bucket}}".accounts_metadata
		where accounts_metadata.accounts_address = new.address
		order by revision desc
		limit 1
	), new.updated_at, new.metadata);

	return new;
end;
$$;

create or replace function "{{.Bucket}}".insert_account_metadata_history() returns trigger
	security definer
	language plpgsql
as
$$
begin
	insert into "{{.Bucket}}".accounts_metadata (ledger, accounts_address, revision, date, metadata)
	values (new.ledger, new.address, 1, new.insertion_date, new.metadata);

	return new;
end;
$$;

create or replace function "{{.Bucket}}".explode_address(_address varchar)
	returns jsonb
	language sql
	immutable
as
$$
select public.aggregate_objects(jsonb_build_object(data.number - 1, data.value))
from (select row_number() over () as number, v.value
      from (select unnest(string_to_array(_address, ':')) as value
            union all
            select null) v) data
$$;

create or replace function "{{.Bucket}}".set_transaction_addresses() returns trigger
	security definer
	language plpgsql
as
$$
begin

	new.sources = (
		select to_jsonb(array_agg(v->>'source')) as value
		from jsonb_array_elements(new.postings::jsonb) v
	);
	new.destinations = (
		select to_jsonb(array_agg(v->>'destination')) as value
		from jsonb_array_elements(new.postings::jsonb) v
	);

	return new;
end
$$;

create or replace function "{{.Bucket}}".set_transaction_addresses_segments() returns trigger
	security definer
	language plpgsql
as
$$
begin
	new.sources_arrays = (
		select to_jsonb(array_agg("{{.Bucket}}".explode_address(v ->> 'source'))) as value
		from jsonb_array_elements(new.postings::jsonb) v
	);
	new.destinations_arrays = (
		select to_jsonb(array_agg("{{.Bucket}}".explode_address(v ->> 'destination'))) as value
		from jsonb_array_elements(new.postings::jsonb) v
	);

	return new;
end
$$;