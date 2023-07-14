--statement
create schema "VAR_LEDGER_NAME_v2_0_0";

--statement
create function "VAR_LEDGER_NAME_v2_0_0".meta_compare(metadata jsonb, value boolean, variadic path text[]) returns boolean
    language plpgsql immutable
    as $$ begin return jsonb_extract_path(metadata, variadic path)::bool = value::bool; exception when others then raise info 'Error Name: %', SQLERRM; raise info 'Error State: %', SQLSTATE; return false; END $$;

--statement
create function "VAR_LEDGER_NAME_v2_0_0".meta_compare(metadata jsonb, value numeric, variadic path text[]) returns boolean
    language plpgsql immutable
    as $$ begin return jsonb_extract_path(metadata, variadic path)::numeric = value::numeric; exception when others then raise info 'Error Name: %', SQLERRM; raise info 'Error State: %', SQLSTATE; return false; END $$;

--statement
create function "VAR_LEDGER_NAME_v2_0_0".meta_compare(metadata jsonb, value character varying, variadic path text[]) returns boolean
    language plpgsql immutable
    as $$ begin return jsonb_extract_path_text(metadata, variadic path)::varchar = value::varchar; exception when others then raise info 'Error Name: %', SQLERRM; raise info 'Error State: %', SQLSTATE; return false; END $$;

--statement
create table "VAR_LEDGER_NAME_v2_0_0".accounts (
    address character varying not null primary key,
    address_json jsonb not null,
    metadata jsonb default '{}'::jsonb
);

--statement
create table "VAR_LEDGER_NAME_v2_0_0".logs (
    id numeric primary key ,
    type smallint,
    hash bytea,
    date timestamp with time zone,
    data jsonb,
    idempotency_key varchar(255),
    projected boolean default false
);

--statement
create table "VAR_LEDGER_NAME_v2_0_0".migrations (
    version character varying primary key,
    date character varying
);

--statement
create table "VAR_LEDGER_NAME_v2_0_0".transactions (
    id bigint unique primary key ,
    "timestamp" timestamp with time zone not null,
    reference character varying unique,
    metadata jsonb default '{}'::jsonb
);

--statement
create table "VAR_LEDGER_NAME_v2_0_0".moves (
    posting_index int8,
    transaction_id bigint,
    account varchar,
    account_array jsonb not null,
    asset varchar,
    post_commit_input_value numeric,
    post_commit_output_value numeric,
    timestamp timestamp with time zone,
    amount numeric not null,
    is_source boolean,

    primary key (transaction_id, posting_index, is_source)
);

--statement
create index logsv2_type on "VAR_LEDGER_NAME_v2_0_0".logs (type);

--statement
create index logsv2_projected on "VAR_LEDGER_NAME_v2_0_0".logs (projected);

--statement
create index logsv2_data on "VAR_LEDGER_NAME_v2_0_0".logs using gin (data);

--statement
create index logsv2_new_transaction_postings on "VAR_LEDGER_NAME_v2_0_0".logs using gin ((data->'transaction'->'postings') jsonb_path_ops);

--statement
create index logsv2_set_metadata on "VAR_LEDGER_NAME_v2_0_0".logs using btree (type, (data->>'targetId'), (data->>'targetType'));

--statement
create index transactions_id_timestamp on "VAR_LEDGER_NAME_v2_0_0".transactions(id, timestamp);

--statement
create index transactions_timestamp on "VAR_LEDGER_NAME_v2_0_0".transactions(timestamp);

--statement
create index transactions_reference on "VAR_LEDGER_NAME_v2_0_0".transactions(reference);

--statement
create index transactions_metadata on "VAR_LEDGER_NAME_v2_0_0".transactions using gin(metadata);

--statement
create index moves_transaction_id on "VAR_LEDGER_NAME_v2_0_0".moves(transaction_id, posting_index);

--statement
create index moves_account_array on "VAR_LEDGER_NAME_v2_0_0".moves using gin(account_array);

--statement
create index moves_account on "VAR_LEDGER_NAME_v2_0_0".moves(account, asset, timestamp);

--statement
create index moves_is_source on "VAR_LEDGER_NAME_v2_0_0".moves(account, is_source);

--statement
create index accounts_address_json on "VAR_LEDGER_NAME_v2_0_0".accounts using GIN(address_json);

--statement
create function "VAR_LEDGER_NAME_v2_0_0".first_agg (anyelement, anyelement)
  returns anyelement
  language sql immutable strict parallel safe as
'select $1';

--statement
create aggregate "VAR_LEDGER_NAME_v2_0_0".first (anyelement) (
  sfunc    = "VAR_LEDGER_NAME_v2_0_0".first_agg
, stype    = anyelement
, parallel = safe
);

--statement
create aggregate "VAR_LEDGER_NAME_v2_0_0".aggregate_objects(jsonb) (
  sfunc = jsonb_concat,
  stype = jsonb,
  initcond = '{}'
);
