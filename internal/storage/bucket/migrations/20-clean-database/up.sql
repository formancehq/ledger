set search_path = '{{.Bucket}}';

-- Clean all useless function/aggregates/indexes inherited from stateful version.
drop aggregate aggregate_objects(jsonb);
drop aggregate first(anyelement);

drop function array_distinct(anyarray);
drop function insert_posting(_transaction_seq bigint, _ledger character varying, _insertion_date timestamp without time zone, _effective_date timestamp without time zone, posting jsonb, _account_metadata jsonb);
drop function upsert_account(_ledger character varying, _address character varying, _metadata jsonb, _date timestamp without time zone, _first_usage timestamp without time zone);
drop function get_latest_move_for_account_and_asset(_ledger character varying, _account_address character varying, _asset character varying, _before timestamp without time zone);
drop function update_transaction_metadata(_ledger character varying, _id numeric, _metadata jsonb, _date timestamp without time zone);
drop function delete_account_metadata(_ledger character varying, _address character varying, _key character varying, _date timestamp without time zone);
drop function delete_transaction_metadata(_ledger character varying, _id numeric, _key character varying, _date timestamp without time zone);
drop function balance_from_volumes(v volumes);
drop function get_all_account_volumes(_ledger character varying, _account character varying, _before timestamp without time zone);
drop function first_agg(anyelement, anyelement);
drop function volumes_to_jsonb(v volumes_with_asset);
drop function get_account_aggregated_effective_volumes(_ledger character varying, _account_address character varying, _before timestamp without time zone);
drop function handle_log();
drop function get_account_aggregated_volumes(_ledger character varying, _account_address character varying, _before timestamp without time zone);
drop function get_aggregated_volumes_for_transaction(_ledger character varying, tx numeric);
drop function insert_move(_transactions_seq bigint, _ledger character varying, _insertion_date timestamp without time zone, _effective_date timestamp without time zone, _account_address character varying, _asset character varying, _amount numeric, _is_source boolean, _account_exists boolean);
drop function get_all_assets(_ledger character varying);
drop function insert_transaction(_ledger character varying, data jsonb, _date timestamp without time zone, _account_metadata jsonb);
drop function get_all_account_effective_volumes(_ledger character varying, _account character varying, _before timestamp without time zone);
drop function get_account_balance(_ledger character varying, _account character varying, _asset character varying, _before timestamp without time zone);
drop function get_aggregated_effective_volumes_for_transaction(_ledger character varying, tx numeric);
drop function aggregate_ledger_volumes(_ledger character varying, _before timestamp without time zone, _accounts character varying[], _assets character varying[] );
drop function get_transaction(_ledger character varying, _id numeric, _before timestamp without time zone);
drop function revert_transaction(_ledger character varying, _id numeric, _date timestamp without time zone);

drop index transactions_sources_arrays;
drop index transactions_destinations_arrays;
drop index accounts_address_array;
drop index accounts_address_array_length;
drop index transactions_sources;
drop index transactions_destinations;

-- We will remove some triggers writing these columns (set_compat_xxx) later in this file.
-- When these triggers will be removed, there is a little moment where the columns will not be filled and constraints
-- still checked by the database.
-- So, we drop the not null constraint before removing the triggers.
-- Once the triggers removed, we will be able to drop the columns.
alter table moves
alter column transactions_seq drop not null,
alter column accounts_seq drop not null,
alter column accounts_address_array drop not null;

alter table transactions_metadata
alter column transactions_seq drop not null;

alter table accounts_metadata
alter column accounts_seq drop not null;

-- Now, the columns are nullable, we can drop the trigger
drop trigger set_compat_on_move on moves;
drop trigger set_compat_on_accounts_metadata on accounts_metadata;
drop trigger set_compat_on_transactions_metadata on transactions_metadata;
drop function set_compat_on_move();
drop function set_compat_on_accounts_metadata();
drop function set_compat_on_transactions_metadata();

-- Finally remove the columns
alter table moves
drop column transactions_seq,
drop column accounts_seq,
drop column accounts_address_array;

alter table transactions_metadata
drop column transactions_seq;

alter table accounts_metadata
drop column accounts_seq;

alter table transactions
drop column seq;

alter table accounts
drop column seq;

-- rename index create in previous migration, as the drop of the column seq of accounts table has automatically dropped the index accounts_ledger
alter index accounts_ledger2
rename to accounts_ledger;

create or replace function set_log_hash()
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
	order by id desc
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

alter table logs
drop column seq;