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
    "metadata" jsonb   NOT NULL,

    UNIQUE ("address")
);
--statement
ALTER TABLE "VAR_LEDGER_NAME".transactions
    ADD COLUMN "postings" jsonb NOT NULL;
--statement
ALTER TABLE "VAR_LEDGER_NAME".transactions
    ADD COLUMN "metadata" jsonb;
--statement
ALTER TABLE "VAR_LEDGER_NAME".transactions
ALTER COLUMN id TYPE char(36);
--statement
CREATE TABLE IF NOT EXISTS "VAR_LEDGER_NAME".log
(
    "id"   bigint,
    "type" varchar,
    "hash" varchar,
    "date" date,
    "data" json,

    UNIQUE ("id")
);
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
DROP TRIGGER IF EXISTS balances_changed ON "VAR_LEDGER_NAME".volumes;
--statement
CREATE TRIGGER balances_changed
    AFTER INSERT OR UPDATE
    ON "VAR_LEDGER_NAME".volumes
    FOR EACH ROW
EXECUTE PROCEDURE "VAR_LEDGER_NAME".update_balances();
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
DROP TRIGGER IF EXISTS volumes_changed ON "VAR_LEDGER_NAME".transactions;
--statement
CREATE TRIGGER volumes_changed
    AFTER INSERT
    ON "VAR_LEDGER_NAME".transactions
    FOR EACH ROW
EXECUTE PROCEDURE "VAR_LEDGER_NAME".update_volumes();
--statement
CREATE INDEX IF NOT EXISTS balances_account ON "VAR_LEDGER_NAME".balances ("account");
--statement
CREATE INDEX IF NOT EXISTS volumes_account ON "VAR_LEDGER_NAME".volumes ("account");
--statement
CREATE OR REPLACE FUNCTION "VAR_LEDGER_NAME".handle_log_entry()
    RETURNS TRIGGER
    LANGUAGE PLPGSQL
AS
$$
BEGIN
    if NEW.type = 'NEW_TRANSACTION' THEN
        INSERT INTO "VAR_LEDGER_NAME".transactions(id, timestamp, reference, postings, metadata)
        VALUES ((NEW.data ->> 'txid')::varchar,
                (NEW.data ->> 'timestamp')::varchar,
                CASE
                    WHEN (NEW.data ->> 'reference')::varchar = '' THEN NULL
                    ELSE (NEW.data ->> 'reference')::varchar END,
                (NEW.data ->> 'postings')::jsonb,
                CASE WHEN (NEW.data ->> 'metadata')::json IS NULL THEN '{}' ELSE (NEW.data ->> 'metadata')::json END);
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
DROP TRIGGER IF EXISTS log_entry ON "VAR_LEDGER_NAME".log;
--statement
CREATE TRIGGER log_entry
    AFTER INSERT
    ON "VAR_LEDGER_NAME".log
    FOR EACH ROW
EXECUTE PROCEDURE "VAR_LEDGER_NAME".handle_log_entry();