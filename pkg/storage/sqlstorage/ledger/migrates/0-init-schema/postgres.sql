--statement
CREATE SCHEMA IF NOT EXISTS "VAR_LEDGER_NAME";

--statement
CREATE FUNCTION "VAR_LEDGER_NAME".meta_compare(metadata jsonb, value boolean, VARIADIC path text[]) RETURNS boolean
    LANGUAGE plpgsql IMMUTABLE
    AS $$ BEGIN return jsonb_extract_path(metadata, variadic path)::bool = value::bool; EXCEPTION WHEN others THEN RAISE INFO 'Error Name: %', SQLERRM; RAISE INFO 'Error State: %', SQLSTATE; RETURN false; END $$;

--statement
CREATE FUNCTION "VAR_LEDGER_NAME".meta_compare(metadata jsonb, value numeric, VARIADIC path text[]) RETURNS boolean
    LANGUAGE plpgsql IMMUTABLE
    AS $$ BEGIN return jsonb_extract_path(metadata, variadic path)::numeric = value::numeric; EXCEPTION WHEN others THEN RAISE INFO 'Error Name: %', SQLERRM; RAISE INFO 'Error State: %', SQLSTATE; RETURN false; END $$;

--statement
CREATE FUNCTION "VAR_LEDGER_NAME".meta_compare(metadata jsonb, value character varying, VARIADIC path text[]) RETURNS boolean
    LANGUAGE plpgsql IMMUTABLE
    AS $$ BEGIN return jsonb_extract_path_text(metadata, variadic path)::varchar = value::varchar; EXCEPTION WHEN others THEN RAISE INFO 'Error Name: %', SQLERRM; RAISE INFO 'Error State: %', SQLSTATE; RETURN false; END $$;

--statement
CREATE FUNCTION "VAR_LEDGER_NAME".use_account_as_destination(postings jsonb, account character varying) RETURNS boolean
    LANGUAGE sql
    AS $_$ select bool_or(v.value::bool) from ( select jsonb_extract_path_text(jsonb_array_elements(postings), 'destination') ~ ('^' || account || '$') as value) as v; $_$;

--statement
CREATE FUNCTION "VAR_LEDGER_NAME".use_account_as_source(postings jsonb, account character varying) RETURNS boolean
    LANGUAGE sql
    AS $_$ select bool_or(v.value::bool) from ( select jsonb_extract_path_text(jsonb_array_elements(postings), 'source') ~ ('^' || account || '$') as value) as v; $_$;

--statement
CREATE FUNCTION "VAR_LEDGER_NAME".use_account(postings jsonb, account character varying) RETURNS boolean
    LANGUAGE sql
    AS $$ SELECT bool_or(v.value) from ( SELECT "VAR_LEDGER_NAME".use_account_as_source(postings, account) AS value UNION SELECT "VAR_LEDGER_NAME".use_account_as_destination(postings, account) AS value ) v $$;

--statement
CREATE TABLE IF NOT EXISTS "VAR_LEDGER_NAME".accounts (
    address character varying NOT NULL,
    metadata jsonb DEFAULT '{}'::jsonb,

    unique(address)
);

--statement
CREATE TABLE IF NOT EXISTS "VAR_LEDGER_NAME".logs_ingestion (
    onerow_id boolean DEFAULT true NOT NULL,
    log_id bigint,

    primary key (onerow_id)
);

--statement
CREATE TABLE IF NOT EXISTS "VAR_LEDGER_NAME".logs_v2 (
    id bigint,
    type smallint,
    hash character varying(256),
    date timestamp with time zone,
    data bytea,
    reference text,

    unique(id)
);

--statement
CREATE TABLE IF NOT EXISTS "VAR_LEDGER_NAME".migrations (
    version character varying,
    date character varying,

    unique(version)
);

--statement
CREATE TABLE IF NOT EXISTS "VAR_LEDGER_NAME".postings (
    txid uuid,
    posting_index integer,
    source jsonb,
    destination jsonb
);

--statement
CREATE TABLE IF NOT EXISTS "VAR_LEDGER_NAME".transactions (
    id uuid unique,
    "timestamp" timestamp with time zone,
    reference character varying unique,
    hash character varying,
    postings jsonb,
    metadata jsonb DEFAULT '{}'::jsonb,
    pre_commit_volumes jsonb,
    post_commit_volumes jsonb
);

--statement
CREATE TABLE IF NOT EXISTS "VAR_LEDGER_NAME".volumes (
    account character varying,
    asset character varying,
    input numeric,
    output numeric,

    unique(account, asset)
);

--statement
CREATE INDEX IF NOT EXISTS postings_addresses ON "VAR_LEDGER_NAME".transactions USING gin (postings);

--statement
CREATE INDEX IF NOT EXISTS postings_dest ON "VAR_LEDGER_NAME".postings USING gin (destination);

--statement
CREATE INDEX IF NOT EXISTS postings_src ON "VAR_LEDGER_NAME".postings USING gin (source);

--statement
CREATE INDEX IF NOT EXISTS postings_txid ON "VAR_LEDGER_NAME".postings USING btree (txid);
