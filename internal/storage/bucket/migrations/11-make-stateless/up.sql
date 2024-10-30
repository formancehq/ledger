set search_path = '{{.Schema}}';

create or replace function transaction_date() returns timestamp as $$
    declare
        ret timestamp without time zone;
    begin
        create temporary table if not exists transaction_date on commit drop as
        select statement_timestamp();

        select *
        from transaction_date
        limit 1
        into ret;

        if not found then
            ret = statement_timestamp();

            insert into transaction_date
            select ret;
        end if;

        return ret;
    end
$$ language plpgsql;

drop trigger insert_account  on accounts;
drop trigger update_account  on accounts;
drop trigger insert_transaction  on transactions;
drop trigger update_transaction  on transactions;
drop trigger insert_log on logs;

alter table moves
add column transactions_id bigint,
alter column post_commit_volumes drop not null,
alter column post_commit_effective_volumes drop not null,
alter column insertion_date set default (transaction_date() at time zone 'utc'),
alter column effective_date set default (transaction_date() at time zone 'utc'),
alter column account_address_array drop not null;

alter table moves
rename column account_address to accounts_address;

alter table moves
rename column account_address_array to accounts_address_array;

-- since the column `account_address` has been renamed to `accounts_address`, we need to update the function
create or replace function get_aggregated_volumes_for_transaction(_ledger varchar, tx numeric) returns jsonb
    stable
    language sql
as
$$
select aggregate_objects(jsonb_build_object(data.accounts_address, data.aggregated))
from (
    select distinct on (move.accounts_address, move.asset)
        move.accounts_address,
        volumes_to_jsonb((move.asset, first(move.post_commit_volumes))) as aggregated
    from (select * from moves order by seq desc) move
    where move.transactions_seq = tx and
          ledger = _ledger
      group by move.accounts_address, move.asset
) data
$$ set search_path from current;

create or replace function get_aggregated_effective_volumes_for_transaction(_ledger varchar, tx numeric) returns jsonb
    stable
    language sql
as
$$
select aggregate_objects(jsonb_build_object(data.accounts_address, data.aggregated))
from (
    select distinct on (move.accounts_address, move.asset)
        move.accounts_address,
        volumes_to_jsonb((move.asset, first(move.post_commit_effective_volumes))) as aggregated
    from (select * from moves order by seq desc) move
    where move.transactions_seq = tx
        and ledger = _ledger
    group by move.accounts_address, move.asset
) data
$$ set search_path from current;

create or replace function get_all_account_effective_volumes(_ledger varchar, _account varchar, _before timestamp default null)
    returns setof volumes_with_asset
    language sql
    stable
as
$$
with all_assets as (select v.v as asset
                    from get_all_assets(_ledger) v),
     moves as (select m.*
               from all_assets assets
                        join lateral (
                   select *
                   from moves s
                   where (_before is null or s.effective_date <= _before)
                     and s.accounts_address = _account
                     and s.asset = assets.asset
                     and s.ledger = _ledger
                   order by effective_date desc, seq desc
                   limit 1
                   ) m on true)
select moves.asset, moves.post_commit_effective_volumes
from moves
$$ set search_path from current;

create or replace function get_all_account_volumes(_ledger varchar, _account varchar, _before timestamp default null)
    returns setof volumes_with_asset
    language sql
    stable
as
$$
with all_assets as (select v.v as asset
                    from get_all_assets(_ledger) v),
     moves as (select m.*
               from all_assets assets
                        join lateral (
                   select *
                   from moves s
                   where (_before is null or s.insertion_date <= _before)
                     and s.accounts_address = _account
                     and s.asset = assets.asset
                     and s.ledger = _ledger
                   order by seq desc
                   limit 1
                   ) m on true)
select moves.asset, moves.post_commit_volumes
from moves
$$ set search_path from current;

-- notes(gfyrag): temporary trigger to be able to handle writes on the old schema (the code does not specify this anymore)
create or replace function set_compat_on_move()
	returns trigger
	security definer
	language plpgsql
as
$$
begin
	new.transactions_seq = (
		select seq
		from transactions
		where id = new.transactions_id and ledger = new.ledger
	);
	new.accounts_seq = (
		select seq
		from accounts
		where address = new.accounts_address and ledger = new.ledger
	);
	new.accounts_address_array = to_json(string_to_array(new.accounts_address, ':'));

	return new;
end;
$$ set search_path from current;

create trigger set_compat_on_move
before insert on moves
for each row
execute procedure set_compat_on_move();

create or replace function set_compat_on_accounts_metadata()
	returns trigger
	security definer
	language plpgsql
as
$$
begin
	new.accounts_seq = (
		select seq
		from accounts
		where address = new.accounts_address and ledger = new.ledger
	);

	return new;
end;
$$ set search_path from current;

create trigger set_compat_on_accounts_metadata
before insert on accounts_metadata
for each row
execute procedure set_compat_on_accounts_metadata();

create or replace function set_compat_on_transactions_metadata()
	returns trigger
	security definer
	language plpgsql
as
$$
begin
	new.transactions_seq = (
		select seq
		from transactions
		where id = new.transactions_id and ledger = new.ledger
	);

	return new;
end;
$$ set search_path from current;

create trigger set_compat_on_transactions_metadata
before insert on transactions_metadata
for each row
execute procedure set_compat_on_transactions_metadata();

alter table transactions
add column post_commit_volumes jsonb,
add column inserted_at timestamp without time zone default (transaction_date() at time zone 'utc'),
alter column timestamp set default (transaction_date() at time zone 'utc'),
alter column id type bigint;

drop index transactions_reference;
create unique index transactions_reference on transactions (ledger, reference);
create index transactions_sequences on transactions (id, seq);

alter table logs
add column memento bytea,
add column idempotency_hash bytea,
alter column hash drop not null,
alter column date set default (transaction_date() at time zone 'utc');

alter table accounts
alter column address_array drop not null,
alter column first_usage set default (transaction_date() at time zone 'utc'),
alter column insertion_date set default (transaction_date() at time zone 'utc'),
alter column updated_at set default (transaction_date() at time zone 'utc')
;

create table accounts_volumes (
    ledger varchar not null,
    accounts_address varchar not null,
    asset varchar not null,
	input numeric not null,
	output numeric not null,

    primary key (ledger, accounts_address, asset)
);

create index accounts_sequences on accounts (address, seq);

alter table transactions_metadata
add column transactions_id bigint;

alter table accounts_metadata
add column accounts_address varchar;

create function set_effective_volumes()
    returns trigger
    security definer
    language plpgsql
as
$$
begin
    new.post_commit_effective_volumes = coalesce((
        select (
            (post_commit_effective_volumes).inputs + case when new.is_source then 0 else new.amount end,
            (post_commit_effective_volumes).outputs + case when new.is_source then new.amount else 0 end
        )
        from moves
        where accounts_address = new.accounts_address
            and asset = new.asset
            and ledger = new.ledger
            and (effective_date < new.effective_date or (effective_date = new.effective_date and seq < new.seq))
        order by effective_date desc, seq desc
        limit 1
    ), (
        case when new.is_source then 0 else new.amount end,
        case when new.is_source then new.amount else 0 end
    ));

    return new;
end;
$$ set search_path from current;

create function update_effective_volumes()
    returns trigger
    security definer
    language plpgsql
as
$$
begin
    update moves
    set post_commit_effective_volumes = (
		(post_commit_effective_volumes).inputs + case when new.is_source then 0 else new.amount end,
		(post_commit_effective_volumes).outputs + case when new.is_source then new.amount else 0 end
    )
    where accounts_address = new.accounts_address
        and asset = new.asset
        and effective_date > new.effective_date
        and ledger = new.ledger;

    return new;
end;
$$ set search_path from current;

create or replace function update_transaction_metadata_history() returns trigger
	security definer
	language plpgsql
as
$$
begin
	insert into transactions_metadata (ledger, transactions_id, revision, date, metadata)
	values (new.ledger, new.id, (
		select revision + 1
		from transactions_metadata
		where transactions_metadata.transactions_id = new.id and transactions_metadata.ledger = new.ledger
		order by revision desc
		limit 1
	), new.updated_at, new.metadata);

	return new;
end;
$$ set search_path from current;

create or replace function insert_transaction_metadata_history() returns trigger
	security definer
	language plpgsql
as
$$
begin
	insert into transactions_metadata (ledger, transactions_id, revision, date, metadata)
	values (new.ledger, new.id, 1, new.timestamp, new.metadata);

	return new;
end;
$$ set search_path from current;

create or replace function update_account_metadata_history() returns trigger
	security definer
	language plpgsql
as
$$
begin
	insert into accounts_metadata (ledger, accounts_address, revision, date, metadata)
	values (new.ledger, new.address, (
		select revision + 1
		from accounts_metadata
		where accounts_metadata.accounts_address = new.address
		order by revision desc
		limit 1
	), new.updated_at, new.metadata);

	return new;
end;
$$ set search_path from current;

create or replace function insert_account_metadata_history() returns trigger
	security definer
	language plpgsql
as
$$
begin
	insert into accounts_metadata (ledger, accounts_address, revision, date, metadata)
	values (new.ledger, new.address, 1, new.insertion_date, new.metadata);

	return new;
end;
$$ set search_path from current;

create or replace function explode_address(_address varchar)
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
$$ set search_path from current;

create or replace function set_transaction_addresses() returns trigger
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
$$ set search_path from current;

create or replace function set_transaction_addresses_segments() returns trigger
	security definer
	language plpgsql
as
$$
begin
	new.sources_arrays = (
		select to_jsonb(array_agg(explode_address(v ->> 'source'))) as value
		from jsonb_array_elements(new.postings::jsonb) v
	);
	new.destinations_arrays = (
		select to_jsonb(array_agg(explode_address(v ->> 'destination'))) as value
		from jsonb_array_elements(new.postings::jsonb) v
	);

	return new;
end
$$ set search_path from current;

create or replace function set_address_array_for_account() returns trigger
	security definer
	language plpgsql
as
$$
begin
	new.address_array = to_json(string_to_array(new.address, ':'));

	return new;
end
$$ set search_path from current;

create function set_log_hash()
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
	from logs
	where ledger = new.ledger
	order by seq desc
	limit 1;

	-- select only fields participating in the hash on the backend and format json representation the same way
	select '{' ||
		'"type":"' || new.type || '",' ||
		'"data":' || encode(new.memento, 'escape') || ',' ||
		'"date":"' || (to_json(new.date::timestamp)#>>'{}') || 'Z",' ||
		'"idempotencyKey":"' || coalesce(new.idempotency_key, '') || '",' ||
		'"id":0,' ||
		'"hash":null' ||
   '}' into marshalledAsJSON;

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
$$ set search_path from current;

DO
$do$
	declare
		ledger record;
		vsql text;
	BEGIN
		for ledger in select * from _system.ledgers where bucket = current_schema loop
			-- create a sequence for transactions by ledger instead of a sequence of the table as we want to have contiguous ids
			-- notes: we can still have "holes" on ids since a sql transaction can be reverted after a usage of the sequence

			vsql = 'create sequence "transaction_id_' || ledger.id || '" owned by transactions.id';
			execute vsql;

			vsql = 'select setval(''"transaction_id_' || ledger.id || '"'', coalesce((select max(id) + 1 from transactions where ledger = ''' || ledger.name || '''), 1)::bigint, false)';
			raise info '%', vsql;
			execute vsql;

			-- create a sequence for logs by ledger instead of a sequence of the table as we want to have contiguous ids
			-- notes: we can still have "holes" on id since a sql transaction can be reverted after a usage of the sequence
			vsql = 'create sequence "log_id_' || ledger.id || '" owned by logs.id';
			execute vsql;

			vsql = 'select setval(''"log_id_' || ledger.id || '"'', coalesce((select max(id) + 1 from logs where ledger = ''' || ledger.name || '''), 1)::bigint, false)';
			execute vsql;

			-- enable post commit effective volumes synchronously
			vsql = 'create index "pcev_' || ledger.id || '" on moves (accounts_address, asset, effective_date desc) where ledger = ''' || ledger.name || '''';
			execute vsql;

			vsql = 'create trigger "set_effective_volumes_' || ledger.id || '" before insert on moves for each row when (new.ledger = ''' || ledger.name || ''') execute procedure set_effective_volumes()';
			execute vsql;

			vsql = 'create trigger "update_effective_volumes_' || ledger.id || '" after insert on moves for each row when (new.ledger = ''' || ledger.name || ''') execute procedure update_effective_volumes()';
			execute vsql;

			-- logs hash
			vsql = 'create trigger "set_log_hash_' || ledger.id || '" before insert on logs for each row when (new.ledger = ''' || ledger.name || ''') execute procedure set_log_hash()';
			execute vsql;

			vsql = 'create trigger "update_account_metadata_history_' || ledger.id || '" after update on "accounts" for each row when (new.ledger = ''' || ledger.name || ''') execute procedure update_account_metadata_history()';
			execute vsql;

			vsql = 'create trigger "insert_account_metadata_history_' || ledger.id || '" after insert on "accounts" for each row when (new.ledger = ''' || ledger.name || ''') execute procedure insert_account_metadata_history()';
			execute vsql;

			vsql = 'create trigger "update_transaction_metadata_history_' || ledger.id || '" after update on "transactions" for each row when (new.ledger = ''' || ledger.name || ''') execute procedure update_transaction_metadata_history()';
			execute vsql;

			vsql = 'create trigger "insert_transaction_metadata_history_' || ledger.id || '" after insert on "transactions" for each row when (new.ledger = ''' || ledger.name || ''') execute procedure insert_transaction_metadata_history()';
			execute vsql;

			vsql = 'create index "transactions_sources_' || ledger.id || '" on transactions using gin (sources jsonb_path_ops) where ledger = ''' || ledger.name || '''';
			execute vsql;

			vsql = 'create index "transactions_destinations_' || ledger.id || '" on transactions using gin (destinations jsonb_path_ops) where ledger = ''' || ledger.name || '''';
			execute vsql;

			vsql = 'create trigger "transaction_set_addresses_' || ledger.id || '" before insert on transactions for each row when (new.ledger = ''' || ledger.name || ''') execute procedure set_transaction_addresses()';
			execute vsql;

			vsql = 'create index "accounts_address_array_' || ledger.id || '" on accounts using gin (address_array jsonb_ops) where ledger = ''' || ledger.name || '''';
			execute vsql;

			vsql = 'create index "accounts_address_array_length_' || ledger.id || '" on accounts (jsonb_array_length(address_array)) where ledger = ''' || ledger.name || '''';
			execute vsql;

			vsql = 'create trigger "accounts_set_address_array_' || ledger.id || '" before insert on accounts for each row when (new.ledger = ''' || ledger.name || ''') execute procedure set_address_array_for_account()';
			execute vsql;

			vsql = 'create index "transactions_sources_arrays_' || ledger.id || '" on transactions using gin (sources_arrays jsonb_path_ops) where ledger = ''' || ledger.name || '''';
			execute vsql;

			vsql = 'create index "transactions_destinations_arrays_' || ledger.id || '" on transactions using gin (destinations_arrays jsonb_path_ops) where ledger = ''' || ledger.name || '''';
			execute vsql;

			vsql = 'create trigger "transaction_set_addresses_segments_' || ledger.id || '"	before insert on "transactions" for each row when (new.ledger = ''' || ledger.name || ''') execute procedure set_transaction_addresses_segments()';
			execute vsql;
		end loop;
	END
$do$;
