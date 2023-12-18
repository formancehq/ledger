--
-- PostgreSQL database dump
--

-- Dumped from database version 13.8
-- Dumped by pg_dump version 16.1

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: _system; Type: SCHEMA; Schema: -
--

CREATE SCHEMA _system;

--
-- Name: default; Type: SCHEMA; Schema: -
--

CREATE SCHEMA "default";

--
-- Name: public; Type: SCHEMA; Schema: -
--

-- *not* creating schema, since initdb creates it

--
-- Name: wallets-002; Type: SCHEMA; Schema: -
--

CREATE SCHEMA "wallets-002";

--
-- Name: pg_trgm; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public;


--
-- Name: EXTENSION pg_trgm; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION pg_trgm IS 'text similarity measurement and index searching based on trigrams';


--
-- Name: pgcrypto; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public;


--
-- Name: EXTENSION pgcrypto; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION pgcrypto IS 'cryptographic functions';


--
-- Name: compute_hashes(); Type: FUNCTION; Schema: default
--

CREATE FUNCTION "default".compute_hashes() RETURNS void
    LANGUAGE plpgsql
    AS $$ DECLARE r record; BEGIN /* Create JSON object manually as it needs to be in canonical form */ FOR r IN (select id, '{"data":' || "default".normaliz(data::jsonb) || ',"date":"' || to_char (date at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') || '","hash":"","id":' || id || ',"type":"' || type || '"}' as canonical from "default".log) LOOP UPDATE "default".log set hash = (select encode(digest( COALESCE((select '{"data":' || "default".normaliz(data::jsonb) || ',"date":"' || to_char (date at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') || '","hash":"' || hash || '","id":' || id || ',"type":"' || type || '"}' from "default".log where id = r.id - 1), 'null') || r.canonical, 'sha256' ), 'hex')) WHERE id = r.id; END LOOP; END $$;

--
-- Name: compute_volumes(); Type: FUNCTION; Schema: default
--

CREATE FUNCTION "default".compute_volumes() RETURNS trigger
    LANGUAGE plpgsql
    AS $$ DECLARE p record; BEGIN FOR p IN ( SELECT t.postings->>'source' as source, t.postings->>'asset' as asset, sum ((t.postings->>'amount')::bigint) as amount FROM ( SELECT jsonb_array_elements(((newtable.data::jsonb)->>'postings')::jsonb) as postings FROM newtable WHERE newtable.type = 'NEW_TRANSACTION' ) t GROUP BY source, asset ) LOOP INSERT INTO "default".accounts (address, metadata) VALUES (p.source, '{}') ON CONFLICT DO NOTHING; INSERT INTO "default".volumes (account, asset, input, output) VALUES (p.source, p.asset, 0, p.amount::bigint) ON CONFLICT (account, asset) DO UPDATE SET output = p.amount::bigint + ( SELECT output FROM "default".volumes WHERE account = p.source AND asset = p.asset ); END LOOP; FOR p IN ( SELECT t.postings->>'destination' as destination, t.postings->>'asset' as asset, sum ((t.postings->>'amount')::bigint) as amount FROM ( SELECT jsonb_array_elements(((newtable.data::jsonb)->>'postings')::jsonb) as postings FROM newtable WHERE newtable.type = 'NEW_TRANSACTION' ) t GROUP BY destination, asset ) LOOP INSERT INTO "default".accounts (address, metadata) VALUES (p.destination, '{}') ON CONFLICT DO NOTHING; INSERT INTO "default".volumes (account, asset, input, output) VALUES (p.destination, p.asset, p.amount::bigint, 0) ON CONFLICT (account, asset) DO UPDATE SET input = p.amount::bigint + ( SELECT input FROM "default".volumes WHERE account = p.destination AND asset = p.asset ); END LOOP; RETURN NULL; END $$;


--
-- Name: handle_log_entry(); Type: FUNCTION; Schema: default
--

CREATE FUNCTION "default".handle_log_entry() RETURNS trigger
    LANGUAGE plpgsql
    AS $$ BEGIN if NEW.type = 'NEW_TRANSACTION' THEN INSERT INTO "default".transactions(id, timestamp, reference, postings, metadata, pre_commit_volumes, post_commit_volumes) VALUES ( (NEW.data ->> 'txid')::bigint, (NEW.data ->> 'timestamp')::varchar, CASE WHEN (NEW.data ->> 'reference')::varchar = '' THEN NULL ELSE (NEW.data ->> 'reference')::varchar END, (NEW.data ->> 'postings')::jsonb, CASE WHEN (NEW.data ->> 'metadata')::jsonb IS NULL THEN '{}' ELSE (NEW.data ->> 'metadata')::jsonb END, (NEW.data ->> 'preCommitVolumes')::jsonb, (NEW.data ->> 'postCommitVolumes')::jsonb ); END IF; if NEW.type = 'SET_METADATA' THEN if NEW.data ->> 'targetType' = 'TRANSACTION' THEN UPDATE "default".transactions SET metadata = metadata || (NEW.data ->> 'metadata')::jsonb WHERE id = (NEW.data ->> 'targetId')::bigint; END IF; if NEW.data ->> 'targetType' = 'ACCOUNT' THEN INSERT INTO "default".accounts (address, metadata) VALUES ((NEW.data ->> 'targetId')::varchar, (NEW.data ->> 'metadata')::jsonb) ON CONFLICT (address) DO UPDATE SET metadata = accounts.metadata || (NEW.data ->> 'metadata')::jsonb; END IF; END IF; RETURN NEW; END; $$;


--
-- Name: is_valid_json(text); Type: FUNCTION; Schema: default
--

CREATE FUNCTION "default".is_valid_json(p_json text) RETURNS boolean
    LANGUAGE plpgsql IMMUTABLE
    AS $$ BEGIN RETURN (p_json::jsonb IS NOT NULL); EXCEPTION WHEN others THEN RETURN false; END; $$;


--
-- Name: meta_compare(jsonb, boolean, text[]); Type: FUNCTION; Schema: default
--

CREATE FUNCTION "default".meta_compare(metadata jsonb, value boolean, VARIADIC path text[]) RETURNS boolean
    LANGUAGE plpgsql IMMUTABLE
    AS $$ BEGIN return jsonb_extract_path(metadata, variadic path)::bool = value::bool; EXCEPTION WHEN others THEN RAISE INFO 'Error Name: %', SQLERRM; RAISE INFO 'Error State: %', SQLSTATE; RETURN false; END $$;

--
-- Name: meta_compare(jsonb, numeric, text[]); Type: FUNCTION; Schema: default
--

CREATE FUNCTION "default".meta_compare(metadata jsonb, value numeric, VARIADIC path text[]) RETURNS boolean
    LANGUAGE plpgsql IMMUTABLE
    AS $$ BEGIN return jsonb_extract_path(metadata, variadic path)::numeric = value::numeric; EXCEPTION WHEN others THEN RAISE INFO 'Error Name: %', SQLERRM; RAISE INFO 'Error State: %', SQLSTATE; RETURN false; END $$;

--
-- Name: meta_compare(jsonb, character varying, text[]); Type: FUNCTION; Schema: default
--

CREATE FUNCTION "default".meta_compare(metadata jsonb, value character varying, VARIADIC path text[]) RETURNS boolean
    LANGUAGE plpgsql IMMUTABLE
    AS $$ BEGIN return jsonb_extract_path_text(metadata, variadic path)::varchar = value::varchar; EXCEPTION WHEN others THEN RAISE INFO 'Error Name: %', SQLERRM; RAISE INFO 'Error State: %', SQLSTATE; RETURN false; END $$;

--
-- Name: normaliz(jsonb); Type: FUNCTION; Schema: default
--

CREATE FUNCTION "default".normaliz(v jsonb) RETURNS text
    LANGUAGE plpgsql
    AS $$ DECLARE r record; t jsonb; BEGIN if jsonb_typeof(v) = 'object' then return ( SELECT COALESCE('{' || string_agg(keyValue, ',') || '}', '{}') FROM ( SELECT '"' || key || '":' || value as keyValue FROM ( SELECT key, (CASE WHEN "default".is_valid_json((select v ->> key)) THEN (select "default".normaliz((select v ->> key)::jsonb)) ELSE '"' || (select v ->> key) || '"' END) as value FROM ( SELECT jsonb_object_keys(v) as key ) t order by key ) t ) t ); end if; if jsonb_typeof(v) = 'array' then return ( select COALESCE('[' || string_agg(items, ',') || ']', '[]') from ( select "default".normaliz(item) as items from jsonb_array_elements(v) item ) t ); end if; if jsonb_typeof(v) = 'string' then return v::text; end if; if jsonb_typeof(v) = 'number' then return v::bigint; end if; if jsonb_typeof(v) = 'boolean' then return v::boolean; end if; return ''; END $$;

--
-- Name: use_account(jsonb, character varying); Type: FUNCTION; Schema: default
--

CREATE FUNCTION "default".use_account(postings jsonb, account character varying) RETURNS boolean
    LANGUAGE sql
    AS $$ SELECT bool_or(v.value) from ( SELECT "default".use_account_as_source(postings, account) AS value UNION SELECT "default".use_account_as_destination(postings, account) AS value ) v $$;

--
-- Name: use_account_as_destination(jsonb, character varying); Type: FUNCTION; Schema: default
--

CREATE FUNCTION "default".use_account_as_destination(postings jsonb, account character varying) RETURNS boolean
    LANGUAGE sql
    AS $_$ select bool_or(v.value::bool) from ( select jsonb_extract_path_text(jsonb_array_elements(postings), 'destination') ~ ('^' || account || '$') as value) as v; $_$;

--
-- Name: use_account_as_source(jsonb, character varying); Type: FUNCTION; Schema: default
--

CREATE FUNCTION "default".use_account_as_source(postings jsonb, account character varying) RETURNS boolean
    LANGUAGE sql
    AS $_$ select bool_or(v.value::bool) from ( select jsonb_extract_path_text(jsonb_array_elements(postings), 'source') ~ ('^' || account || '$') as value) as v; $_$;

--
-- Name: compute_hashes(); Type: FUNCTION; Schema: wallets-002
--

CREATE FUNCTION "wallets-002".compute_hashes() RETURNS void
    LANGUAGE plpgsql
    AS $$ DECLARE r record; BEGIN /* Create JSON object manually as it needs to be in canonical form */ FOR r IN (select id, '{"data":' || "wallets-002".normaliz(data::jsonb) || ',"date":"' || to_char (date at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') || '","hash":"","id":' || id || ',"type":"' || type || '"}' as canonical from "wallets-002".log) LOOP UPDATE "wallets-002".log set hash = (select encode(digest( COALESCE((select '{"data":' || "wallets-002".normaliz(data::jsonb) || ',"date":"' || to_char (date at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') || '","hash":"' || hash || '","id":' || id || ',"type":"' || type || '"}' from "wallets-002".log where id = r.id - 1), 'null') || r.canonical, 'sha256' ), 'hex')) WHERE id = r.id; END LOOP; END $$;

--
-- Name: compute_volumes(); Type: FUNCTION; Schema: wallets-002
--

CREATE FUNCTION "wallets-002".compute_volumes() RETURNS trigger
    LANGUAGE plpgsql
    AS $$ DECLARE p record; BEGIN FOR p IN ( SELECT t.postings->>'source' as source, t.postings->>'asset' as asset, sum ((t.postings->>'amount')::bigint) as amount FROM ( SELECT jsonb_array_elements(((newtable.data::jsonb)->>'postings')::jsonb) as postings FROM newtable WHERE newtable.type = 'NEW_TRANSACTION' ) t GROUP BY source, asset ) LOOP INSERT INTO "wallets-002".accounts (address, metadata) VALUES (p.source, '{}') ON CONFLICT DO NOTHING; INSERT INTO "wallets-002".volumes (account, asset, input, output) VALUES (p.source, p.asset, 0, p.amount::bigint) ON CONFLICT (account, asset) DO UPDATE SET output = p.amount::bigint + ( SELECT output FROM "wallets-002".volumes WHERE account = p.source AND asset = p.asset ); END LOOP; FOR p IN ( SELECT t.postings->>'destination' as destination, t.postings->>'asset' as asset, sum ((t.postings->>'amount')::bigint) as amount FROM ( SELECT jsonb_array_elements(((newtable.data::jsonb)->>'postings')::jsonb) as postings FROM newtable WHERE newtable.type = 'NEW_TRANSACTION' ) t GROUP BY destination, asset ) LOOP INSERT INTO "wallets-002".accounts (address, metadata) VALUES (p.destination, '{}') ON CONFLICT DO NOTHING; INSERT INTO "wallets-002".volumes (account, asset, input, output) VALUES (p.destination, p.asset, p.amount::bigint, 0) ON CONFLICT (account, asset) DO UPDATE SET input = p.amount::bigint + ( SELECT input FROM "wallets-002".volumes WHERE account = p.destination AND asset = p.asset ); END LOOP; RETURN NULL; END $$;

--
-- Name: handle_log_entry(); Type: FUNCTION; Schema: wallets-002
--

CREATE FUNCTION "wallets-002".handle_log_entry() RETURNS trigger
    LANGUAGE plpgsql
    AS $$ BEGIN if NEW.type = 'NEW_TRANSACTION' THEN INSERT INTO "wallets-002".transactions(id, timestamp, reference, postings, metadata, pre_commit_volumes, post_commit_volumes) VALUES ( (NEW.data ->> 'txid')::bigint, (NEW.data ->> 'timestamp')::varchar, CASE WHEN (NEW.data ->> 'reference')::varchar = '' THEN NULL ELSE (NEW.data ->> 'reference')::varchar END, (NEW.data ->> 'postings')::jsonb, CASE WHEN (NEW.data ->> 'metadata')::jsonb IS NULL THEN '{}' ELSE (NEW.data ->> 'metadata')::jsonb END, (NEW.data ->> 'preCommitVolumes')::jsonb, (NEW.data ->> 'postCommitVolumes')::jsonb ); END IF; if NEW.type = 'SET_METADATA' THEN if NEW.data ->> 'targetType' = 'TRANSACTION' THEN UPDATE "wallets-002".transactions SET metadata = metadata || (NEW.data ->> 'metadata')::jsonb WHERE id = (NEW.data ->> 'targetId')::bigint; END IF; if NEW.data ->> 'targetType' = 'ACCOUNT' THEN INSERT INTO "wallets-002".accounts (address, metadata) VALUES ((NEW.data ->> 'targetId')::varchar, (NEW.data ->> 'metadata')::jsonb) ON CONFLICT (address) DO UPDATE SET metadata = accounts.metadata || (NEW.data ->> 'metadata')::jsonb; END IF; END IF; RETURN NEW; END; $$;

--
-- Name: is_valid_json(text); Type: FUNCTION; Schema: wallets-002
--

CREATE FUNCTION "wallets-002".is_valid_json(p_json text) RETURNS boolean
    LANGUAGE plpgsql IMMUTABLE
    AS $$ BEGIN RETURN (p_json::jsonb IS NOT NULL); EXCEPTION WHEN others THEN RETURN false; END; $$;

--
-- Name: meta_compare(jsonb, boolean, text[]); Type: FUNCTION; Schema: wallets-002
--

CREATE FUNCTION "wallets-002".meta_compare(metadata jsonb, value boolean, VARIADIC path text[]) RETURNS boolean
    LANGUAGE plpgsql IMMUTABLE
    AS $$ BEGIN return jsonb_extract_path(metadata, variadic path)::bool = value::bool; EXCEPTION WHEN others THEN RAISE INFO 'Error Name: %', SQLERRM; RAISE INFO 'Error State: %', SQLSTATE; RETURN false; END $$;

--
-- Name: meta_compare(jsonb, numeric, text[]); Type: FUNCTION; Schema: wallets-002
--

CREATE FUNCTION "wallets-002".meta_compare(metadata jsonb, value numeric, VARIADIC path text[]) RETURNS boolean
    LANGUAGE plpgsql IMMUTABLE
    AS $$ BEGIN return jsonb_extract_path(metadata, variadic path)::numeric = value::numeric; EXCEPTION WHEN others THEN RAISE INFO 'Error Name: %', SQLERRM; RAISE INFO 'Error State: %', SQLSTATE; RETURN false; END $$;

--
-- Name: meta_compare(jsonb, character varying, text[]); Type: FUNCTION; Schema: wallets-002
--

CREATE FUNCTION "wallets-002".meta_compare(metadata jsonb, value character varying, VARIADIC path text[]) RETURNS boolean
    LANGUAGE plpgsql IMMUTABLE
    AS $$ BEGIN return jsonb_extract_path_text(metadata, variadic path)::varchar = value::varchar; EXCEPTION WHEN others THEN RAISE INFO 'Error Name: %', SQLERRM; RAISE INFO 'Error State: %', SQLSTATE; RETURN false; END $$;

--
-- Name: normaliz(jsonb); Type: FUNCTION; Schema: wallets-002
--

CREATE FUNCTION "wallets-002".normaliz(v jsonb) RETURNS text
    LANGUAGE plpgsql
    AS $$ DECLARE r record; t jsonb; BEGIN if jsonb_typeof(v) = 'object' then return ( SELECT COALESCE('{' || string_agg(keyValue, ',') || '}', '{}') FROM ( SELECT '"' || key || '":' || value as keyValue FROM ( SELECT key, (CASE WHEN "wallets-002".is_valid_json((select v ->> key)) THEN (select "wallets-002".normaliz((select v ->> key)::jsonb)) ELSE '"' || (select v ->> key) || '"' END) as value FROM ( SELECT jsonb_object_keys(v) as key ) t order by key ) t ) t ); end if; if jsonb_typeof(v) = 'array' then return ( select COALESCE('[' || string_agg(items, ',') || ']', '[]') from ( select "wallets-002".normaliz(item) as items from jsonb_array_elements(v) item ) t ); end if; if jsonb_typeof(v) = 'string' then return v::text; end if; if jsonb_typeof(v) = 'number' then return v::bigint; end if; if jsonb_typeof(v) = 'boolean' then return v::boolean; end if; return ''; END $$;

--
-- Name: use_account(jsonb, character varying); Type: FUNCTION; Schema: wallets-002
--

CREATE FUNCTION "wallets-002".use_account(postings jsonb, account character varying) RETURNS boolean
    LANGUAGE sql
    AS $$ SELECT bool_or(v.value) from ( SELECT "wallets-002".use_account_as_source(postings, account) AS value UNION SELECT "wallets-002".use_account_as_destination(postings, account) AS value ) v $$;

--
-- Name: use_account_as_destination(jsonb, character varying); Type: FUNCTION; Schema: wallets-002
--

CREATE FUNCTION "wallets-002".use_account_as_destination(postings jsonb, account character varying) RETURNS boolean
    LANGUAGE sql
    AS $_$ select bool_or(v.value::bool) from ( select jsonb_extract_path_text(jsonb_array_elements(postings), 'destination') ~ ('^' || account || '$') as value) as v; $_$;

--
-- Name: use_account_as_source(jsonb, character varying); Type: FUNCTION; Schema: wallets-002
--

CREATE FUNCTION "wallets-002".use_account_as_source(postings jsonb, account character varying) RETURNS boolean
    LANGUAGE sql
    AS $_$ select bool_or(v.value::bool) from ( select jsonb_extract_path_text(jsonb_array_elements(postings), 'source') ~ ('^' || account || '$') as value) as v; $_$;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: configuration; Type: TABLE; Schema: _system
--

CREATE TABLE _system.configuration (
    key character varying(255) NOT NULL,
    value text,
    addedat timestamp without time zone
);

--
-- Name: ledgers; Type: TABLE; Schema: _system
--

CREATE TABLE _system.ledgers (
    ledger character varying(255) NOT NULL,
    addedat timestamp without time zone
);

--
-- Name: accounts; Type: TABLE; Schema: default
--

CREATE TABLE "default".accounts (
    address character varying NOT NULL,
    metadata jsonb DEFAULT '{}'::jsonb,
    address_json jsonb
);

--
-- Name: idempotency; Type: TABLE; Schema: default
--

CREATE TABLE "default".idempotency (
    key character varying NOT NULL,
    date character varying,
    status_code integer,
    headers character varying,
    body character varying,
    request_hash character varying
);

--
-- Name: log; Type: TABLE; Schema: default
--

CREATE TABLE "default".log (
    id bigint,
    type character varying,
    hash character varying,
    date timestamp with time zone,
    data jsonb
);

--
-- Name: log_seq; Type: SEQUENCE; Schema: default
--

CREATE SEQUENCE "default".log_seq
    START WITH 0
    INCREMENT BY 1
    MINVALUE 0
    NO MAXVALUE
    CACHE 1;

--
-- Name: mapping; Type: TABLE; Schema: default
--

CREATE TABLE "default".mapping (
    mapping_id character varying,
    mapping character varying
);

--
-- Name: migrations; Type: TABLE; Schema: default
--

CREATE TABLE "default".migrations (
    version character varying,
    date character varying
);

--
-- Name: postings; Type: TABLE; Schema: default
--

CREATE TABLE "default".postings (
    txid bigint,
    posting_index integer,
    source jsonb,
    destination jsonb
);

--
-- Name: transactions; Type: TABLE; Schema: default
--

CREATE TABLE "default".transactions (
    id bigint,
    "timestamp" timestamp with time zone,
    reference character varying,
    hash character varying,
    postings jsonb,
    metadata jsonb DEFAULT '{}'::jsonb,
    pre_commit_volumes jsonb,
    post_commit_volumes jsonb
);

--
-- Name: volumes; Type: TABLE; Schema: default
--

CREATE TABLE "default".volumes (
    account character varying,
    asset character varying,
    input numeric,
    output numeric,
    account_json jsonb
);

--
-- Name: accounts; Type: TABLE; Schema: wallets-002
--

CREATE TABLE "wallets-002".accounts (
    address character varying NOT NULL,
    metadata jsonb DEFAULT '{}'::jsonb,
    address_json jsonb
);

--
-- Name: idempotency; Type: TABLE; Schema: wallets-002
--

CREATE TABLE "wallets-002".idempotency (
    key character varying NOT NULL,
    date character varying,
    status_code integer,
    headers character varying,
    body character varying,
    request_hash character varying
);

--
-- Name: log; Type: TABLE; Schema: wallets-002
--

CREATE TABLE "wallets-002".log (
    id bigint,
    type character varying,
    hash character varying,
    date timestamp with time zone,
    data jsonb
);

--
-- Name: log_seq; Type: SEQUENCE; Schema: wallets-002
--

CREATE SEQUENCE "wallets-002".log_seq
    START WITH 0
    INCREMENT BY 1
    MINVALUE 0
    NO MAXVALUE
    CACHE 1;

--
-- Name: mapping; Type: TABLE; Schema: wallets-002
--

CREATE TABLE "wallets-002".mapping (
    mapping_id character varying,
    mapping character varying
);

--
-- Name: migrations; Type: TABLE; Schema: wallets-002
--

CREATE TABLE "wallets-002".migrations (
    version character varying,
    date character varying
);

--
-- Name: postings; Type: TABLE; Schema: wallets-002
--

CREATE TABLE "wallets-002".postings (
    txid bigint,
    posting_index integer,
    source jsonb,
    destination jsonb
);

--
-- Name: transactions; Type: TABLE; Schema: wallets-002
--

CREATE TABLE "wallets-002".transactions (
    id bigint,
    "timestamp" timestamp with time zone,
    reference character varying,
    hash character varying,
    postings jsonb,
    metadata jsonb DEFAULT '{}'::jsonb,
    pre_commit_volumes jsonb,
    post_commit_volumes jsonb
);

--
-- Name: volumes; Type: TABLE; Schema: wallets-002
--

CREATE TABLE "wallets-002".volumes (
    account character varying,
    asset character varying,
    input numeric,
    output numeric,
    account_json jsonb
);

--
-- Data for Name: configuration; Type: TABLE DATA; Schema: _system
--

INSERT INTO _system.configuration (key, value, addedat) VALUES ('appId', '7f50ba54-cdb1-4e79-a2f7-3e704ce08d08', '2023-12-13 18:16:31');


--
-- Data for Name: ledgers; Type: TABLE DATA; Schema: _system
--

INSERT INTO _system.ledgers (ledger, addedat) VALUES ('wallets-002', '2023-12-13 18:16:35.943038');
INSERT INTO _system.ledgers (ledger, addedat) VALUES ('default', '2023-12-13 18:21:05.044237');


--
-- Data for Name: accounts; Type: TABLE DATA; Schema: default
--

INSERT INTO "default".accounts (address, metadata, address_json) VALUES ('world', '{}', '["world"]');
INSERT INTO "default".accounts (address, metadata, address_json) VALUES ('bank', '{}', '["bank"]');
INSERT INTO "default".accounts (address, metadata, address_json) VALUES ('bob', '{}', '["bob"]');
INSERT INTO "default".accounts (address, metadata, address_json) VALUES ('alice', '{"foo": "bar"}', '["alice"]');


--
-- Data for Name: idempotency; Type: TABLE DATA; Schema: default
--



--
-- Data for Name: log; Type: TABLE DATA; Schema: default
--

INSERT INTO "default".log (id, type, hash, date, data) VALUES (0, 'NEW_TRANSACTION', '79fc36b46f2668ee1f682a109765af8e849d11715d078bd361e7b4eb61fadc70', '2023-12-13 18:21:05+00', '{"txid": 0, "metadata": {}, "postings": [{"asset": "USD/2", "amount": 10000, "source": "world", "destination": "bank"}], "reference": "", "timestamp": "2023-12-13T18:21:05Z"}');
INSERT INTO "default".log (id, type, hash, date, data) VALUES (1, 'NEW_TRANSACTION', 'e493bab4fcce0c281193414ea43a7d34b73c89ac1bb103755e9fb1064d00c0e8', '2023-12-13 18:21:40+00', '{"txid": 1, "metadata": {}, "postings": [{"asset": "USD/2", "amount": 10000, "source": "world", "destination": "bob"}], "reference": "", "timestamp": "2023-12-13T18:21:40Z"}');
INSERT INTO "default".log (id, type, hash, date, data) VALUES (2, 'NEW_TRANSACTION', '19ac0ffff69a271615ba09c6564f3851ab0fe32e7aabe3ab9083b63501f29332', '2023-12-13 18:21:46+00', '{"txid": 2, "metadata": {}, "postings": [{"asset": "USD/2", "amount": 10000, "source": "world", "destination": "alice"}], "reference": "", "timestamp": "2023-12-13T18:21:46Z"}');
INSERT INTO "default".log (id, type, hash, date, data) VALUES (3, 'SET_METADATA', '839800b3bf685903b37240e8a59e1872d29c2ed9715a79c56b86edb5b5b0976f', '2023-12-14 09:30:31+00', '{"metadata": {"foo": "bar"}, "targetId": "alice", "targetType": "ACCOUNT"}');


--
-- Data for Name: mapping; Type: TABLE DATA; Schema: default
--



--
-- Data for Name: migrations; Type: TABLE DATA; Schema: default
--

INSERT INTO "default".migrations (version, date) VALUES ('0', '2023-12-13T18:21:05Z');
INSERT INTO "default".migrations (version, date) VALUES ('1', '2023-12-13T18:21:05Z');
INSERT INTO "default".migrations (version, date) VALUES ('2', '2023-12-13T18:21:05Z');
INSERT INTO "default".migrations (version, date) VALUES ('3', '2023-12-13T18:21:05Z');
INSERT INTO "default".migrations (version, date) VALUES ('4', '2023-12-13T18:21:05Z');
INSERT INTO "default".migrations (version, date) VALUES ('5', '2023-12-13T18:21:05Z');
INSERT INTO "default".migrations (version, date) VALUES ('6', '2023-12-13T18:21:05Z');
INSERT INTO "default".migrations (version, date) VALUES ('7', '2023-12-13T18:21:05Z');
INSERT INTO "default".migrations (version, date) VALUES ('8', '2023-12-13T18:21:05Z');
INSERT INTO "default".migrations (version, date) VALUES ('9', '2023-12-13T18:21:05Z');
INSERT INTO "default".migrations (version, date) VALUES ('10', '2023-12-13T18:21:05Z');
INSERT INTO "default".migrations (version, date) VALUES ('11', '2023-12-13T18:21:05Z');
INSERT INTO "default".migrations (version, date) VALUES ('12', '2023-12-13T18:21:05Z');
INSERT INTO "default".migrations (version, date) VALUES ('13', '2023-12-13T18:21:05Z');
INSERT INTO "default".migrations (version, date) VALUES ('14', '2023-12-13T18:21:05Z');
INSERT INTO "default".migrations (version, date) VALUES ('15', '2023-12-13T18:21:05Z');
INSERT INTO "default".migrations (version, date) VALUES ('16', '2023-12-13T18:21:05Z');
INSERT INTO "default".migrations (version, date) VALUES ('17', '2023-12-13T18:21:05Z');
INSERT INTO "default".migrations (version, date) VALUES ('18', '2023-12-13T18:21:05Z');
INSERT INTO "default".migrations (version, date) VALUES ('19', '2023-12-13T18:21:05Z');
INSERT INTO "default".migrations (version, date) VALUES ('20', '2023-12-13T18:21:05Z');


--
-- Data for Name: postings; Type: TABLE DATA; Schema: default
--

INSERT INTO "default".postings (txid, posting_index, source, destination) VALUES (0, 0, '["world"]', '["bank"]');
INSERT INTO "default".postings (txid, posting_index, source, destination) VALUES (1, 0, '["world"]', '["bob"]');
INSERT INTO "default".postings (txid, posting_index, source, destination) VALUES (2, 0, '["world"]', '["alice"]');


--
-- Data for Name: transactions; Type: TABLE DATA; Schema: default
--

INSERT INTO "default".transactions (id, "timestamp", reference, hash, postings, metadata, pre_commit_volumes, post_commit_volumes) VALUES (0, '2023-12-13 18:21:05+00', NULL, NULL, '[{"asset": "USD/2", "amount": 10000, "source": "world", "destination": "bank"}]', '{}', '{"bank": {"USD/2": {"input": 0, "output": 0, "balance": 0}}, "world": {"USD/2": {"input": 0, "output": 0, "balance": 0}}}', '{"bank": {"USD/2": {"input": 10000, "output": 0, "balance": 10000}}, "world": {"USD/2": {"input": 0, "output": 10000, "balance": -10000}}}');
INSERT INTO "default".transactions (id, "timestamp", reference, hash, postings, metadata, pre_commit_volumes, post_commit_volumes) VALUES (1, '2023-12-13 18:21:40+00', NULL, NULL, '[{"asset": "USD/2", "amount": 10000, "source": "world", "destination": "bob"}]', '{}', '{"bob": {"USD/2": {"input": 0, "output": 0, "balance": 0}}, "world": {"USD/2": {"input": 0, "output": 10000, "balance": -10000}}}', '{"bob": {"USD/2": {"input": 10000, "output": 0, "balance": 10000}}, "world": {"USD/2": {"input": 0, "output": 20000, "balance": -20000}}}');
INSERT INTO "default".transactions (id, "timestamp", reference, hash, postings, metadata, pre_commit_volumes, post_commit_volumes) VALUES (2, '2023-12-13 18:21:46+00', NULL, NULL, '[{"asset": "USD/2", "amount": 10000, "source": "world", "destination": "alice"}]', '{}', '{"alice": {"USD/2": {"input": 0, "output": 0, "balance": 0}}, "world": {"USD/2": {"input": 0, "output": 20000, "balance": -20000}}}', '{"alice": {"USD/2": {"input": 10000, "output": 0, "balance": 10000}}, "world": {"USD/2": {"input": 0, "output": 30000, "balance": -30000}}}');


--
-- Data for Name: volumes; Type: TABLE DATA; Schema: default
--

INSERT INTO "default".volumes (account, asset, input, output, account_json) VALUES ('bank', 'USD/2', 10000, 0, '["bank"]');
INSERT INTO "default".volumes (account, asset, input, output, account_json) VALUES ('bob', 'USD/2', 10000, 0, '["bob"]');
INSERT INTO "default".volumes (account, asset, input, output, account_json) VALUES ('alice', 'USD/2', 10000, 0, '["alice"]');
INSERT INTO "default".volumes (account, asset, input, output, account_json) VALUES ('world', 'USD/2', 0, 30000, '["world"]');


--
-- Data for Name: accounts; Type: TABLE DATA; Schema: wallets-002
--

INSERT INTO "wallets-002".accounts (address, metadata, address_json) VALUES ('wallets:15b7a366c6e9473f96276803ef585ae9:main', '{"wallets/id": "15b7a366-c6e9-473f-9627-6803ef585ae9", "wallets/name": "wallet1", "wallets/balances": "true", "wallets/createdAt": "2023-12-14T09:30:48.01540488Z", "wallets/spec/type": "wallets.primary", "wallets/custom_data": {}, "wallets/balances/name": "main"}', '["wallets", "15b7a366c6e9473f96276803ef585ae9", "main"]');
INSERT INTO "wallets-002".accounts (address, metadata, address_json) VALUES ('world', '{}', '["world"]');
INSERT INTO "wallets-002".accounts (address, metadata, address_json) VALUES ('wallets:71e6788ad1954139bec5c3e35ee4a2dc:main', '{"wallets/id": "71e6788a-d195-4139-bec5-c3e35ee4a2dc", "wallets/name": "wallet2", "wallets/balances": "true", "wallets/createdAt": "2023-12-14T09:32:38.001913219Z", "wallets/spec/type": "wallets.primary", "wallets/custom_data": {"catgory": "gold"}, "wallets/balances/name": "main"}', '["wallets", "71e6788ad1954139bec5c3e35ee4a2dc", "main"]');


--
-- Data for Name: idempotency; Type: TABLE DATA; Schema: wallets-002
--



--
-- Data for Name: log; Type: TABLE DATA; Schema: wallets-002
--

INSERT INTO "wallets-002".log (id, type, hash, date, data) VALUES (0, 'SET_METADATA', 'c3d4b844838f4feaf0d35f1f37f8eae496b66328a69fc3d73e46a7cd53b231b6', '2023-12-14 09:30:48+00', '{"metadata": {"wallets/id": "15b7a366-c6e9-473f-9627-6803ef585ae9", "wallets/name": "wallet1", "wallets/balances": "true", "wallets/createdAt": "2023-12-14T09:30:48.01540488Z", "wallets/spec/type": "wallets.primary", "wallets/custom_data": {}, "wallets/balances/name": "main"}, "targetId": "wallets:15b7a366c6e9473f96276803ef585ae9:main", "targetType": "ACCOUNT"}');
INSERT INTO "wallets-002".log (id, type, hash, date, data) VALUES (1, 'NEW_TRANSACTION', '1f2d8e75e937cee1c91e0a2696f5fbe59947d77ad568cf45c58a01430acb5f0b', '2023-12-14 09:32:04+00', '{"txid": 0, "metadata": {"wallets/custom_data": {}, "wallets/transaction": "true"}, "postings": [{"asset": "USD/2", "amount": 100, "source": "world", "destination": "wallets:15b7a366c6e9473f96276803ef585ae9:main"}], "reference": "", "timestamp": "2023-12-14T09:32:04Z"}');
INSERT INTO "wallets-002".log (id, type, hash, date, data) VALUES (2, 'SET_METADATA', '3665750bbbe64e79c4631927e9399a8c7f817b55d572ef41cfd9714bd679db7d', '2023-12-14 09:32:38+00', '{"metadata": {"wallets/id": "71e6788a-d195-4139-bec5-c3e35ee4a2dc", "wallets/name": "wallet2", "wallets/balances": "true", "wallets/createdAt": "2023-12-14T09:32:38.001913219Z", "wallets/spec/type": "wallets.primary", "wallets/custom_data": {"catgory": "gold"}, "wallets/balances/name": "main"}, "targetId": "wallets:71e6788ad1954139bec5c3e35ee4a2dc:main", "targetType": "ACCOUNT"}');


--
-- Data for Name: mapping; Type: TABLE DATA; Schema: wallets-002
--



--
-- Data for Name: migrations; Type: TABLE DATA; Schema: wallets-002
--

INSERT INTO "wallets-002".migrations (version, date) VALUES ('0', '2023-12-13T18:16:36Z');
INSERT INTO "wallets-002".migrations (version, date) VALUES ('1', '2023-12-13T18:16:36Z');
INSERT INTO "wallets-002".migrations (version, date) VALUES ('2', '2023-12-13T18:16:36Z');
INSERT INTO "wallets-002".migrations (version, date) VALUES ('3', '2023-12-13T18:16:36Z');
INSERT INTO "wallets-002".migrations (version, date) VALUES ('4', '2023-12-13T18:16:36Z');
INSERT INTO "wallets-002".migrations (version, date) VALUES ('5', '2023-12-13T18:16:36Z');
INSERT INTO "wallets-002".migrations (version, date) VALUES ('6', '2023-12-13T18:16:36Z');
INSERT INTO "wallets-002".migrations (version, date) VALUES ('7', '2023-12-13T18:16:36Z');
INSERT INTO "wallets-002".migrations (version, date) VALUES ('8', '2023-12-13T18:16:36Z');
INSERT INTO "wallets-002".migrations (version, date) VALUES ('9', '2023-12-13T18:16:36Z');
INSERT INTO "wallets-002".migrations (version, date) VALUES ('10', '2023-12-13T18:16:36Z');
INSERT INTO "wallets-002".migrations (version, date) VALUES ('11', '2023-12-13T18:16:36Z');
INSERT INTO "wallets-002".migrations (version, date) VALUES ('12', '2023-12-13T18:16:36Z');
INSERT INTO "wallets-002".migrations (version, date) VALUES ('13', '2023-12-13T18:16:36Z');
INSERT INTO "wallets-002".migrations (version, date) VALUES ('14', '2023-12-13T18:16:36Z');
INSERT INTO "wallets-002".migrations (version, date) VALUES ('15', '2023-12-13T18:16:36Z');
INSERT INTO "wallets-002".migrations (version, date) VALUES ('16', '2023-12-13T18:16:36Z');
INSERT INTO "wallets-002".migrations (version, date) VALUES ('17', '2023-12-13T18:16:36Z');
INSERT INTO "wallets-002".migrations (version, date) VALUES ('18', '2023-12-13T18:16:36Z');
INSERT INTO "wallets-002".migrations (version, date) VALUES ('19', '2023-12-13T18:16:36Z');
INSERT INTO "wallets-002".migrations (version, date) VALUES ('20', '2023-12-13T18:16:36Z');


--
-- Data for Name: postings; Type: TABLE DATA; Schema: wallets-002
--

INSERT INTO "wallets-002".postings (txid, posting_index, source, destination) VALUES (0, 0, '["world"]', '["wallets", "15b7a366c6e9473f96276803ef585ae9", "main"]');


--
-- Data for Name: transactions; Type: TABLE DATA; Schema: wallets-002
--

INSERT INTO "wallets-002".transactions (id, "timestamp", reference, hash, postings, metadata, pre_commit_volumes, post_commit_volumes) VALUES (0, '2023-12-14 09:32:04+00', NULL, NULL, '[{"asset": "USD/2", "amount": 100, "source": "world", "destination": "wallets:15b7a366c6e9473f96276803ef585ae9:main"}]', '{"wallets/custom_data": {}, "wallets/transaction": "true"}', '{"world": {"USD/2": {"input": 0, "output": 0, "balance": 0}}, "wallets:15b7a366c6e9473f96276803ef585ae9:main": {"USD/2": {"input": 0, "output": 0, "balance": 0}}}', '{"world": {"USD/2": {"input": 0, "output": 100, "balance": -100}}, "wallets:15b7a366c6e9473f96276803ef585ae9:main": {"USD/2": {"input": 100, "output": 0, "balance": 100}}}');


--
-- Data for Name: volumes; Type: TABLE DATA; Schema: wallets-002
--

INSERT INTO "wallets-002".volumes (account, asset, input, output, account_json) VALUES ('world', 'USD/2', 0, 100, '["world"]');
INSERT INTO "wallets-002".volumes (account, asset, input, output, account_json) VALUES ('wallets:15b7a366c6e9473f96276803ef585ae9:main', 'USD/2', 100, 0, '["wallets", "15b7a366c6e9473f96276803ef585ae9", "main"]');


--
-- Name: log_seq; Type: SEQUENCE SET; Schema: default
--

SELECT pg_catalog.setval('"default".log_seq', 0, false);


--
-- Name: log_seq; Type: SEQUENCE SET; Schema: wallets-002
--

SELECT pg_catalog.setval('"wallets-002".log_seq', 0, false);


--
-- Name: configuration configuration_pkey; Type: CONSTRAINT; Schema: _system
--

ALTER TABLE ONLY _system.configuration
    ADD CONSTRAINT configuration_pkey PRIMARY KEY (key);


--
-- Name: ledgers ledgers_pkey; Type: CONSTRAINT; Schema: _system
--

ALTER TABLE ONLY _system.ledgers
    ADD CONSTRAINT ledgers_pkey PRIMARY KEY (ledger);


--
-- Name: accounts accounts_address_key; Type: CONSTRAINT; Schema: default
--

ALTER TABLE ONLY "default".accounts
    ADD CONSTRAINT accounts_address_key UNIQUE (address);


--
-- Name: idempotency idempotency_pkey; Type: CONSTRAINT; Schema: default
--

ALTER TABLE ONLY "default".idempotency
    ADD CONSTRAINT idempotency_pkey PRIMARY KEY (key);


--
-- Name: log log_id_key; Type: CONSTRAINT; Schema: default
--

ALTER TABLE ONLY "default".log
    ADD CONSTRAINT log_id_key UNIQUE (id);


--
-- Name: mapping mapping_mapping_id_key; Type: CONSTRAINT; Schema: default
--

ALTER TABLE ONLY "default".mapping
    ADD CONSTRAINT mapping_mapping_id_key UNIQUE (mapping_id);


--
-- Name: migrations migrations_version_key; Type: CONSTRAINT; Schema: default
--

ALTER TABLE ONLY "default".migrations
    ADD CONSTRAINT migrations_version_key UNIQUE (version);


--
-- Name: transactions transactions_id_key; Type: CONSTRAINT; Schema: default
--

ALTER TABLE ONLY "default".transactions
    ADD CONSTRAINT transactions_id_key UNIQUE (id);


--
-- Name: transactions transactions_reference_key; Type: CONSTRAINT; Schema: default
--

ALTER TABLE ONLY "default".transactions
    ADD CONSTRAINT transactions_reference_key UNIQUE (reference);


--
-- Name: volumes volumes_account_asset_key; Type: CONSTRAINT; Schema: default
--

ALTER TABLE ONLY "default".volumes
    ADD CONSTRAINT volumes_account_asset_key UNIQUE (account, asset);


--
-- Name: accounts accounts_address_key; Type: CONSTRAINT; Schema: wallets-002
--

ALTER TABLE ONLY "wallets-002".accounts
    ADD CONSTRAINT accounts_address_key UNIQUE (address);


--
-- Name: idempotency idempotency_pkey; Type: CONSTRAINT; Schema: wallets-002
--

ALTER TABLE ONLY "wallets-002".idempotency
    ADD CONSTRAINT idempotency_pkey PRIMARY KEY (key);


--
-- Name: log log_id_key; Type: CONSTRAINT; Schema: wallets-002
--

ALTER TABLE ONLY "wallets-002".log
    ADD CONSTRAINT log_id_key UNIQUE (id);


--
-- Name: mapping mapping_mapping_id_key; Type: CONSTRAINT; Schema: wallets-002
--

ALTER TABLE ONLY "wallets-002".mapping
    ADD CONSTRAINT mapping_mapping_id_key UNIQUE (mapping_id);


--
-- Name: migrations migrations_version_key; Type: CONSTRAINT; Schema: wallets-002
--

ALTER TABLE ONLY "wallets-002".migrations
    ADD CONSTRAINT migrations_version_key UNIQUE (version);


--
-- Name: transactions transactions_id_key; Type: CONSTRAINT; Schema: wallets-002
--

ALTER TABLE ONLY "wallets-002".transactions
    ADD CONSTRAINT transactions_id_key UNIQUE (id);


--
-- Name: transactions transactions_reference_key; Type: CONSTRAINT; Schema: wallets-002
--

ALTER TABLE ONLY "wallets-002".transactions
    ADD CONSTRAINT transactions_reference_key UNIQUE (reference);


--
-- Name: volumes volumes_account_asset_key; Type: CONSTRAINT; Schema: wallets-002
--

ALTER TABLE ONLY "wallets-002".volumes
    ADD CONSTRAINT volumes_account_asset_key UNIQUE (account, asset);


--
-- Name: accounts_address_json; Type: INDEX; Schema: default
--

CREATE INDEX accounts_address_json ON "default".accounts USING gin (address_json);


--
-- Name: accounts_array_length; Type: INDEX; Schema: default
--

CREATE INDEX accounts_array_length ON "default".accounts USING btree (jsonb_array_length(address_json));


--
-- Name: postings_addresses; Type: INDEX; Schema: default
--

CREATE INDEX postings_addresses ON "default".transactions USING gin (postings);


--
-- Name: postings_array_length_dst; Type: INDEX; Schema: default
--

CREATE INDEX postings_array_length_dst ON "default".postings USING btree (jsonb_array_length(destination));


--
-- Name: postings_array_length_src; Type: INDEX; Schema: default
--

CREATE INDEX postings_array_length_src ON "default".postings USING btree (jsonb_array_length(source));


--
-- Name: postings_dest; Type: INDEX; Schema: default
--

CREATE INDEX postings_dest ON "default".postings USING gin (destination);


--
-- Name: postings_src; Type: INDEX; Schema: default
--

CREATE INDEX postings_src ON "default".postings USING gin (source);


--
-- Name: postings_txid; Type: INDEX; Schema: default
--

CREATE INDEX postings_txid ON "default".postings USING btree (txid);


--
-- Name: volumes_account_json; Type: INDEX; Schema: default
--

CREATE INDEX volumes_account_json ON "default".volumes USING gin (account_json);


--
-- Name: volumes_array_length; Type: INDEX; Schema: default
--

CREATE INDEX volumes_array_length ON "default".volumes USING btree (jsonb_array_length(account_json));


--
-- Name: accounts_address_json; Type: INDEX; Schema: wallets-002
--

CREATE INDEX accounts_address_json ON "wallets-002".accounts USING gin (address_json);


--
-- Name: accounts_array_length; Type: INDEX; Schema: wallets-002
--

CREATE INDEX accounts_array_length ON "wallets-002".accounts USING btree (jsonb_array_length(address_json));


--
-- Name: postings_addresses; Type: INDEX; Schema: wallets-002
--

CREATE INDEX postings_addresses ON "wallets-002".transactions USING gin (postings);


--
-- Name: postings_array_length_dst; Type: INDEX; Schema: wallets-002
--

CREATE INDEX postings_array_length_dst ON "wallets-002".postings USING btree (jsonb_array_length(destination));


--
-- Name: postings_array_length_src; Type: INDEX; Schema: wallets-002
--

CREATE INDEX postings_array_length_src ON "wallets-002".postings USING btree (jsonb_array_length(source));


--
-- Name: postings_dest; Type: INDEX; Schema: wallets-002
--

CREATE INDEX postings_dest ON "wallets-002".postings USING gin (destination);


--
-- Name: postings_src; Type: INDEX; Schema: wallets-002
--

CREATE INDEX postings_src ON "wallets-002".postings USING gin (source);


--
-- Name: postings_txid; Type: INDEX; Schema: wallets-002
--

CREATE INDEX postings_txid ON "wallets-002".postings USING btree (txid);


--
-- Name: volumes_account_json; Type: INDEX; Schema: wallets-002
--

CREATE INDEX volumes_account_json ON "wallets-002".volumes USING gin (account_json);


--
-- Name: volumes_array_length; Type: INDEX; Schema: wallets-002
--

CREATE INDEX volumes_array_length ON "wallets-002".volumes USING btree (jsonb_array_length(account_json));


--
-- PostgreSQL database dump complete
--

