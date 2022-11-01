--statement
create table "VAR_LEDGER_NAME".postings (
  txid bigint,
  posting_index integer,
  source jsonb,
  destination jsonb
);
--statement
create index postings_src on "VAR_LEDGER_NAME".postings using GIN(source);
create index postings_dest on "VAR_LEDGER_NAME".postings using GIN(destination);
--statement
insert into "VAR_LEDGER_NAME".postings(txid, posting_index, source, destination)
select
    txs.id as txid,
    i - 1 as posting_index,
    array_to_json(string_to_array(t.posting->>'source', ':'))::jsonb as source,
    array_to_json(string_to_array(t.posting->>'destination', ':'))::jsonb as destination
from "VAR_LEDGER_NAME".transactions txs
left join lateral jsonb_array_elements(txs.postings) with ordinality as t(posting, i) on true;
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
        FOR i IN 0..(SELECT jsonb_array_length((NEW.data ->> 'postings')::jsonb) - 1) LOOP
          INSERT INTO "VAR_LEDGER_NAME".postings (txid, posting_index, source, destination)
          VALUES (
            (NEW.data ->> 'txid')::bigint,
            i,
            array_to_json(string_to_array(((NEW.data ->> 'postings')::jsonb -> i) ->> 'source', ':'))::jsonb,
            array_to_json(string_to_array(((NEW.data ->> 'postings')::jsonb -> i) ->> 'destination', ':'))::jsonb
          );
        END LOOP;
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