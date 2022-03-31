--statement
CREATE OR REPLACE FUNCTION "VAR_LEDGER_NAME".is_valid_json(p_json text)
    RETURNS BOOLEAN
AS
$$
BEGIN
    RETURN (p_json::jsonb IS NOT NULL);
EXCEPTION
    WHEN others THEN
        RETURN false;
END;
$$
    LANGUAGE plpgsql
    IMMUTABLE;
--statement
CREATE OR REPLACE FUNCTION "VAR_LEDGER_NAME".handle_log_entry()
    RETURNS TRIGGER
    LANGUAGE PLPGSQL
AS
$$
BEGIN
    if NEW.type = 'NEW_TRANSACTION' THEN
        INSERT INTO "VAR_LEDGER_NAME".transactions(id, timestamp, reference, postings, metadata)
        VALUES ((NEW.data ->> 'txid')::bigint,
                (NEW.data ->> 'timestamp')::varchar,
                CASE
                    WHEN (NEW.data ->> 'reference')::varchar = '' THEN NULL
                    ELSE (NEW.data ->> 'reference')::varchar END,
                (NEW.data ->> 'postings')::jsonb,
                CASE WHEN (NEW.data ->> 'metadata')::jsonb IS NULL THEN '{}' ELSE (NEW.data ->> 'metadata')::jsonb END);
    END IF;
    if NEW.type = 'SET_METADATA' THEN
        if NEW.data ->> 'targetType' = 'TRANSACTION' THEN
            UPDATE "VAR_LEDGER_NAME".transactions
            SET metadata = metadata || (NEW.data ->> 'metadata')::jsonb
            WHERE id = (NEW.data ->> 'targetId')::bigint;
        END IF;
        if NEW.data ->> 'targetType' = 'ACCOUNT' THEN
            INSERT INTO "VAR_LEDGER_NAME".accounts (address, metadata)
            VALUES ((NEW.data ->> 'targetId')::varchar,
                    (NEW.data ->> 'metadata')::jsonb)
            ON CONFLICT (address) DO UPDATE SET metadata = accounts.metadata || (NEW.data ->> 'metadata')::jsonb;
        END IF;
    END IF;
    RETURN NEW;
END;
$$;
--statement
CREATE TABLE IF NOT EXISTS "VAR_LEDGER_NAME".volumes
(
    "account" varchar,
    "asset"   varchar,
    "input"   bigint,
    "output"  bigint,

    UNIQUE ("account", "asset")
);
--statement
CREATE TABLE IF NOT EXISTS "VAR_LEDGER_NAME".accounts
(
    "address"  varchar NOT NULL,
    "metadata" jsonb   DEFAULT '{}',

    UNIQUE ("address")
);
--statement
ALTER TABLE "VAR_LEDGER_NAME".transactions
    ADD COLUMN "postings" jsonb;
--statement
ALTER TABLE "VAR_LEDGER_NAME".transactions
    ADD COLUMN "metadata" jsonb DEFAULT '{}';
--statement
CREATE TABLE IF NOT EXISTS "VAR_LEDGER_NAME".log
(
    "id"   bigint,
    "type" varchar,
    "hash" varchar,
    "date" timestamp with time zone,
    "data" jsonb,

    UNIQUE ("id")
);
--statement
CREATE INDEX IF NOT EXISTS volumes_account ON "VAR_LEDGER_NAME".volumes ("account");
--statement
UPDATE "VAR_LEDGER_NAME".transactions
SET postings = (
    SELECT ('[' || string_agg(v.j, ',') || ']')::json
    FROM (
             SELECT '{' ||
                    '"amount":' || amount || ',' ||
                    '"asset":"' || asset || '",' ||
                    '"destination":"' || destination || '",' ||
                    '"source":"' || source || '"' ||
                    '}' as j,
                    txid
             FROM "VAR_LEDGER_NAME".postings
             WHERE txid::bigint = transactions.id
             ORDER BY txid DESC
         ) v
);
--statement
CREATE SEQUENCE "VAR_LEDGER_NAME".log_seq START WITH 0 MINVALUE 0;
--statement
INSERT INTO "VAR_LEDGER_NAME".log(id, type, date, data, hash)
SELECT nextval('"VAR_LEDGER_NAME".log_seq'), v.type, v.timestamp::timestamp with time zone, v.data::json, ''
FROM (
     SELECT id as ord, 'NEW_TRANSACTION' as type, timestamp, '{"metadata":{},"postings":' || postings::varchar || ',"reference":"' || CASE WHEN reference IS NOT NULL THEN reference ELSE '' END || '","timestamp":"' || timestamp || '","txid":' || id || '}' as data
     FROM "VAR_LEDGER_NAME".transactions
     UNION ALL
     SELECT 100000000000 + meta_id as ord, 'SET_METADATA' as type, timestamp, '{"metadata":{"' || meta_key || '":' || CASE WHEN "VAR_LEDGER_NAME".is_valid_json(meta_value) THEN meta_value ELSE '"' || meta_value || '"' END || '},"targetId":' || CASE WHEN meta_target_type = 'transaction' THEN meta_target_id ELSE ('"' || meta_target_id || '"') END || ',"targetType":"' || UPPER(meta_target_type) || '"}' as data
     FROM "VAR_LEDGER_NAME".metadata
 ) v
ORDER BY v.timestamp ASC, v.ord ASC;
-- statement
DROP SEQUENCE "VAR_LEDGER_NAME".log_seq;
--statement
UPDATE "VAR_LEDGER_NAME".transactions
SET metadata = (
    SELECT ('{' || COALESCE(STRING_AGG('"' || meta_key || '":' || CASE WHEN "VAR_LEDGER_NAME".is_valid_json(meta_value) THEN meta_value ELSE '"' || meta_value || '"' END, ','), '') || '}')::json
    FROM (
             SELECT DISTINCT ON (meta_key)
                 meta_id, meta_key, meta_value
             FROM "VAR_LEDGER_NAME".metadata
             WHERE meta_target_type = 'transaction' AND meta_target_id::bigint = transactions.id
             ORDER BY meta_key, meta_id DESC
         ) v
);
--statement
INSERT INTO "VAR_LEDGER_NAME".accounts(address) SELECT * FROM "VAR_LEDGER_NAME".addresses;
--statement
UPDATE "VAR_LEDGER_NAME".accounts
SET metadata = (
    SELECT ('{' || string_agg('"' || meta_key || '":' || CASE WHEN "VAR_LEDGER_NAME".is_valid_json(meta_value) THEN meta_value ELSE '"' || meta_value || '"' END, ',') || '}')::json
    FROM (
             SELECT distinct on (meta_key)
                 meta_id, meta_key, meta_value
             FROM "VAR_LEDGER_NAME".metadata
             WHERE meta_target_id = accounts.address
             ORDER BY meta_key, meta_id DESC
         ) v
);
--statement
DROP TRIGGER IF EXISTS log_entry ON "VAR_LEDGER_NAME".log;
--statement
CREATE TRIGGER log_entry
    AFTER INSERT
    ON "VAR_LEDGER_NAME".log
    FOR EACH ROW
EXECUTE PROCEDURE "VAR_LEDGER_NAME".handle_log_entry();
--statement
INSERT INTO "VAR_LEDGER_NAME".volumes (account, asset, input, output)
SELECT destination, asset, SUM(amount), 0
FROM "VAR_LEDGER_NAME".postings
GROUP BY asset, destination;
--statement
INSERT INTO "VAR_LEDGER_NAME".volumes (account, asset, input, output)
SELECT source, asset, 0, SUM(amount)
FROM "VAR_LEDGER_NAME".postings
GROUP BY asset, source
ON CONFLICT (account, asset) DO UPDATE SET output = volumes.output + excluded.output;
--statement
CREATE OR REPLACE FUNCTION "VAR_LEDGER_NAME".compute_volumes() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    p record;
BEGIN
    FOR p IN (
        SELECT
                t.postings->>'source' as source,
                t.postings->>'asset' as asset,
                sum ((t.postings->>'amount')::bigint) as amount
        FROM (
                 SELECT jsonb_array_elements(((newtable.data::jsonb)->>'postings')::jsonb) as postings
                 FROM newtable
                WHERE newtable.type = 'NEW_TRANSACTION'
             ) t
        GROUP BY source, asset
    ) LOOP
            INSERT INTO "VAR_LEDGER_NAME".accounts (address, metadata)
            VALUES (p.source, '{}')
            ON CONFLICT DO NOTHING;

            INSERT INTO "VAR_LEDGER_NAME".volumes (account, asset, input, output)
            VALUES (p.source, p.asset, 0, p.amount::bigint)
            ON CONFLICT (account, asset) DO UPDATE SET output = p.amount::bigint + (
                SELECT output
                FROM "VAR_LEDGER_NAME".volumes
                WHERE account = p.source
                  AND asset = p.asset
            );
        END LOOP;
    FOR p IN (
        SELECT
                t.postings->>'destination' as destination,
                t.postings->>'asset' as asset,
                sum ((t.postings->>'amount')::bigint) as amount
        FROM (
                 SELECT jsonb_array_elements(((newtable.data::jsonb)->>'postings')::jsonb) as postings
                 FROM newtable
                 WHERE newtable.type = 'NEW_TRANSACTION'
             ) t
        GROUP BY destination, asset
    ) LOOP
            INSERT INTO "VAR_LEDGER_NAME".accounts (address, metadata)
            VALUES (p.destination, '{}')
            ON CONFLICT DO NOTHING;

            INSERT INTO "VAR_LEDGER_NAME".volumes (account, asset, input, output)
            VALUES (p.destination, p.asset, p.amount::bigint, 0)
            ON CONFLICT (account, asset) DO UPDATE SET input = p.amount::bigint + (
                SELECT input
                FROM "VAR_LEDGER_NAME".volumes
                WHERE account = p.destination
                  AND asset = p.asset
            );
        END LOOP;
    RETURN NULL;
END
$$;
--statement
CREATE TRIGGER volumes_changed
AFTER INSERT
ON "VAR_LEDGER_NAME".log
REFERENCING NEW TABLE AS newtable
FOR EACH STATEMENT
EXECUTE PROCEDURE "VAR_LEDGER_NAME".compute_volumes();
--statement
CREATE OR REPLACE FUNCTION "VAR_LEDGER_NAME".normaliz(v jsonb)
    RETURNS text AS
$BODY$
DECLARE
    r record;
    t jsonb;
BEGIN
    if jsonb_typeof(v) = 'object' then
        return (
            SELECT COALESCE('{' || string_agg(keyValue, ',') || '}', '{}')
            FROM (
                     SELECT '"' || key || '":' || value as keyValue
                     FROM (
                              SELECT key, (CASE WHEN "VAR_LEDGER_NAME".is_valid_json((select v ->> key)) THEN (select "VAR_LEDGER_NAME".normaliz((select v ->> key)::jsonb)) ELSE '"' || (select v ->> key) || '"' END) as value
                              FROM (
                                       SELECT jsonb_object_keys(v) as key
                                   ) t
                              order by key
                          ) t
                 ) t
        );
    end if;
    if jsonb_typeof(v) = 'array' then
        return (
            select COALESCE('[' || string_agg(items, ',') || ']', '[]')
            from (
                     select "VAR_LEDGER_NAME".normaliz(item) as items from jsonb_array_elements(v) item
                 ) t
        );
    end if;
    if jsonb_typeof(v) = 'string' then
        return v::text;
    end if;
    if jsonb_typeof(v) = 'number' then
        return v::bigint;
    end if;
    if jsonb_typeof(v) = 'boolean' then
        return v::boolean;
    end if;

    return '';
END
$BODY$
    LANGUAGE plpgsql;
--statement
CREATE OR REPLACE FUNCTION "VAR_LEDGER_NAME".compute_hashes()
    RETURNS void AS
$BODY$
DECLARE
    r record;
BEGIN
    -- Create JSON object manually as it needs to be in canonical form
    FOR r IN (select id, '{"data":' || "VAR_LEDGER_NAME".normaliz(data::jsonb) || ',"date":"' || to_char (date at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') || '","hash":"","id":' || id || ',"type":"' || type || '"}' as canonical from "VAR_LEDGER_NAME".log)
    LOOP
        UPDATE "VAR_LEDGER_NAME".log set hash = (select encode(digest(
             COALESCE((select '{"data":' || "VAR_LEDGER_NAME".normaliz(data::jsonb) || ',"date":"' || to_char (date at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') || '","hash":"' || hash || '","id":' || id || ',"type":"' || type || '"}' from "VAR_LEDGER_NAME".log where id = r.id - 1), 'null') || r.canonical,
             'sha256'
        ), 'hex'))
        WHERE id = r.id;
    END LOOP;
END
$BODY$
    LANGUAGE plpgsql;
--statement
SELECT "VAR_LEDGER_NAME".compute_hashes();