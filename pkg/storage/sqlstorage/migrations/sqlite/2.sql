--statement
ALTER TABLE transactions
ADD COLUMN pre_commit_volumes varchar;
--statement
ALTER TABLE transactions
ADD COLUMN post_commit_volumes varchar;
--statement
DROP TRIGGER new_log_transaction;
--statement
CREATE TRIGGER new_log_transaction
AFTER INSERT
ON log
WHEN new.type = 'NEW_TRANSACTION'
BEGIN
INSERT INTO transactions (id, reference, timestamp, postings, metadata, pre_commit_volumes, post_commit_volumes)
VALUES (
        json_extract(new.data, '$.txid'),
        CASE
            WHEN json_extract(new.data, '$.reference') = '' THEN NULL
            ELSE json_extract(new.data, '$.reference') END,
        json_extract(new.data, '$.timestamp'),
        json_extract(new.data, '$.postings'),
        CASE
            WHEN json_extract(new.data, '$.metadata') IS NULL THEN '{}'
            ELSE json_extract(new.data, '$.metadata') END,
        json_extract(new.data, '$.preCommitVolumes'),
        json_extract(new.data, '$.postCommitVolumes')
);
END;
