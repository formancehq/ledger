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
CREATE OR REPLACE FUNCTION "VAR_LEDGER_NAME".update_volumes()
    RETURNS TRIGGER
    LANGUAGE PLPGSQL
AS
$$
DECLARE
    p jsonb;
BEGIN
    FOR p IN (SELECT * FROM jsonb_array_elements(NEW.postings))
        LOOP
            INSERT INTO "VAR_LEDGER_NAME".volumes (account, asset, input, output)
            VALUES (p ->> 'source', p ->> 'asset', 0, (p ->> 'amount')::bigint)
            ON CONFLICT (account, asset) DO UPDATE SET output = (p ->> 'amount')::bigint + (
                SELECT output
                FROM "VAR_LEDGER_NAME".volumes
                WHERE account = p ->> 'source'
                  AND asset = p ->> 'asset'
            );

            INSERT INTO "VAR_LEDGER_NAME".volumes (account, asset, input, output)
            VALUES (p ->> 'destination', p ->> 'asset', (p ->> 'amount')::bigint, 0)
            ON CONFLICT (account, asset) DO UPDATE SET input = (p ->> 'amount')::bigint + (
                SELECT input
                FROM "VAR_LEDGER_NAME".volumes
                WHERE account = p ->> 'destination'
                  AND asset = p ->> 'asset'
            );
            INSERT INTO "VAR_LEDGER_NAME".accounts (address, metadata)
            VALUES (p ->> 'source', '{}')
            ON CONFLICT DO NOTHING;
            INSERT INTO "VAR_LEDGER_NAME".accounts (address, metadata)
            VALUES (p ->> 'destination', '{}')
            ON CONFLICT DO NOTHING;
        END LOOP;
    RETURN NEW;
END;
$$;
--statement
CREATE OR REPLACE FUNCTION "VAR_LEDGER_NAME".update_balances()
    RETURNS TRIGGER
    LANGUAGE PLPGSQL
AS
$$
BEGIN
    INSERT INTO "VAR_LEDGER_NAME".balances (account, asset, amount)
    VALUES (NEW.account, NEW.asset, NEW.input - NEW.output)
    ON CONFLICT (account, asset) DO UPDATE SET amount = NEW.input - NEW.output;
    RETURN NEW;
END;
$$;
--statement
CREATE OR REPLACE FUNCTION "VAR_LEDGER_NAME".handle_log_entry()
    RETURNS TRIGGER
    LANGUAGE PLPGSQL
AS
$$
BEGIN
    if NEW.type = 'NEW_TRANSACTION' THEN
        INSERT INTO "VAR_LEDGER_NAME".transactions(id, timestamp, reference, postings, metadata, ord)
        VALUES ((NEW.data ->> 'txid')::varchar,
                (NEW.data ->> 'timestamp')::varchar,
                CASE
                    WHEN (NEW.data ->> 'reference')::varchar = '' THEN NULL
                    ELSE (NEW.data ->> 'reference')::varchar END,
                (NEW.data ->> 'postings')::jsonb,
                CASE WHEN (NEW.data ->> 'metadata')::jsonb IS NULL THEN '{}' ELSE (NEW.data ->> 'metadata')::jsonb END,
                (SELECT count(*)+1 FROM "VAR_LEDGER_NAME".transactions));
    END IF;
    if NEW.type = 'SET_METADATA' THEN
        if NEW.data ->> 'targetType' = 'TRANSACTION' THEN
            UPDATE "VAR_LEDGER_NAME".transactions
            SET metadata = metadata || (NEW.data ->> 'metadata')::jsonb
            WHERE id = (NEW.data ->> 'targetId')::varchar;
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
CREATE TABLE IF NOT EXISTS "VAR_LEDGER_NAME".balances
(
    "account" varchar,
    "asset"   varchar,
    "amount"  bigint,

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
ALTER TABLE "VAR_LEDGER_NAME".transactions
    ADD COLUMN ord BIGINT; -- Allow to keep ordering
--statement
UPDATE "VAR_LEDGER_NAME".transactions
SET ord = id;
--statement
ALTER TABLE "VAR_LEDGER_NAME".transactions
    ALTER COLUMN id TYPE varchar(36);
--statement
CREATE TABLE IF NOT EXISTS "VAR_LEDGER_NAME".log
(
    "id"   bigint,
    "type" varchar,
    "hash" varchar,
    "date" timestamp with time zone,
    "data" json,

    UNIQUE ("id")
);
--statement
CREATE INDEX IF NOT EXISTS balances_account ON "VAR_LEDGER_NAME".balances ("account");
--statement
CREATE INDEX IF NOT EXISTS volumes_account ON "VAR_LEDGER_NAME".volumes ("account");
--statement
UPDATE "VAR_LEDGER_NAME".transactions
SET postings = (
    SELECT ('[' || string_agg(v.j, ',') || ']')::jsonb
    FROM (
             SELECT '{"source":"' || source || '", "destination":"' || destination || '", "asset":"' || asset ||
                    '","amount": ' || amount || '}' as j,
                    txid
             FROM "VAR_LEDGER_NAME".postings
             WHERE txid::VARCHAR = transactions.id
             ORDER BY txid DESC
         ) v
);
--statement
CREATE SEQUENCE "VAR_LEDGER_NAME".log_seq START WITH 0 MINVALUE 0;
--statement
INSERT INTO "VAR_LEDGER_NAME".log(id, type, date, data, hash)
SELECT nextval('"VAR_LEDGER_NAME".log_seq'), v.type, v.timestamp::timestamp with time zone, v.data, ''
FROM (
         SELECT 0 as ord, ord as ord2, 'NEW_TRANSACTION' as type, timestamp::timestamp with time zone, ('{"txid": "' || id || '", "postings": ' || postings::varchar || ', "metadata": {}, "timestamp": "' || timestamp || '", "reference": "' || CASE WHEN reference IS NOT NULL THEN reference ELSE '' END || '"}')::jsonb as data
         FROM "VAR_LEDGER_NAME".transactions
         UNION ALL
         SELECT meta_id as ord, 0 as ord2, 'SET_METADATA' as type, timestamp::timestamp with time zone, ('{"targetType": "' || UPPER(meta_target_type) || '", "targetId": "' || meta_target_id || '", "metadata": {"' || meta_key || '": ' || CASE WHEN "VAR_LEDGER_NAME".is_valid_json(meta_value) THEN meta_value ELSE '"' || meta_value || '"' END || '}}')::jsonb as data
         FROM "VAR_LEDGER_NAME".metadata
     ) v
ORDER BY v.timestamp ASC, v.ord ASC, v.ord2 ASC;
-- statement
DROP SEQUENCE "VAR_LEDGER_NAME".log_seq;
--statement
UPDATE "VAR_LEDGER_NAME".transactions
SET metadata = (
    SELECT ('{' || COALESCE(STRING_AGG('"' || meta_key || '":' || CASE WHEN "VAR_LEDGER_NAME".is_valid_json(meta_value) THEN meta_value ELSE '"' || meta_value || '"' END, ','), '') || '}')::jsonb
    FROM (
             SELECT DISTINCT ON (meta_key)
                 meta_id, meta_key, meta_value
             FROM "VAR_LEDGER_NAME".metadata
             WHERE meta_target_id = transactions.id
             ORDER BY meta_key, meta_id DESC
         ) v
);
--statement
INSERT INTO "VAR_LEDGER_NAME".accounts(address) SELECT * FROM "VAR_LEDGER_NAME".addresses;
--statement
UPDATE "VAR_LEDGER_NAME".accounts
SET metadata = (
    SELECT ('{' || string_agg('"' || meta_key || '":' || CASE WHEN "VAR_LEDGER_NAME".is_valid_json(meta_value) THEN meta_value ELSE '"' || meta_value || '"' END, ',') || '}')::jsonb
    FROM (
             SELECT distinct on (meta_key)
                 meta_id, meta_key, meta_value
             FROM "VAR_LEDGER_NAME".metadata
             WHERE meta_target_id = accounts.address
             ORDER BY meta_key, meta_id DESC
         ) v
);
--statement
DROP TRIGGER IF EXISTS balances_changed ON "VAR_LEDGER_NAME".volumes;
--statement
CREATE TRIGGER balances_changed
    AFTER INSERT OR UPDATE
    ON "VAR_LEDGER_NAME".volumes
    FOR EACH ROW
EXECUTE PROCEDURE "VAR_LEDGER_NAME".update_balances();
--statement
DROP TRIGGER IF EXISTS volumes_changed ON "VAR_LEDGER_NAME".transactions;
--statement
CREATE TRIGGER volumes_changed
    AFTER INSERT
    ON "VAR_LEDGER_NAME".transactions
    FOR EACH ROW
EXECUTE PROCEDURE "VAR_LEDGER_NAME".update_volumes();
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