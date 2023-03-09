--statement
ALTER TABLE "VAR_LEDGER_NAME".transactions
ADD COLUMN "pre_commit_volumes" jsonb;
--statement
ALTER TABLE "VAR_LEDGER_NAME".transactions
ADD COLUMN "post_commit_volumes" jsonb;
--statement
CREATE OR REPLACE FUNCTION "VAR_LEDGER_NAME".handle_log_entry()
    RETURNS TRIGGER
    LANGUAGE PLPGSQL
AS
$$
BEGIN
    if NEW.type = 'NEW_TRANSACTION' THEN
        INSERT INTO "VAR_LEDGER_NAME".transactions(id, timestamp, reference, postings, metadata, pre_commit_volumes, post_commit_volumes)
        VALUES (
                (NEW.data ->> 'txid')::bigint,
                (NEW.data ->> 'timestamp')::varchar,
                CASE
                    WHEN (NEW.data ->> 'reference')::varchar = '' THEN NULL
                    ELSE (NEW.data ->> 'reference')::varchar END,
                (NEW.data ->> 'postings')::jsonb,
                CASE WHEN (NEW.data ->> 'metadata')::jsonb IS NULL THEN '{}' ELSE (NEW.data ->> 'metadata')::jsonb END,
                (NEW.data ->> 'preCommitVolumes')::jsonb,
                (NEW.data ->> 'postCommitVolumes')::jsonb
        );
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
