--statement
CREATE TABLE IF NOT EXISTS volumes
(
    "account" varchar,
    "asset"   varchar,
    "input"   integer,
    "output"  integer,

    UNIQUE ("account", "asset")
);
--statement
CREATE TABLE IF NOT EXISTS balances
(
    "account" varchar,
    "asset"   varchar,
    "amount"  integer,

    UNIQUE ("account", "asset")
);
--statement
CREATE TABLE IF NOT EXISTS accounts
(
    "address"  varchar NOT NULL,
    "metadata" varchar DEFAULT '{}',

    UNIQUE ("address")
);
--statement
CREATE TABLE transactions_v2 (
                                 "id"        varchar(36) primary key,
                                 "timestamp" varchar,
                                 "reference" varchar,
                                 "hash"      varchar,
                                 "postings" varchar,
                                 "metadata" varchar DEFAULT '{}',
                                 "ord" integer,

                                 UNIQUE("reference"),
                                 UNIQUE("ord")
);
--statement
INSERT INTO transactions_v2 (id, timestamp, reference, hash, ord) SELECT id, timestamp, reference, hash, id from transactions;
--statement
DROP TABLE transactions;
--statement
ALTER TABLE transactions_v2 RENAME TO transactions;
--statement
CREATE TABLE IF NOT EXISTS log
(
    "id"   integer primary key autoincrement,
    "type" varchar,
    "hash" varchar,
    "date" date,
    "data" varchar
);
--statement
UPDATE transactions
SET postings = (
    SELECT '[' || group_concat(v.j) || ']'
    FROM (
             SELECT '{"source": "' || source || '", "destination": "' || destination || '", "asset": "' || asset || '", "amount": ' || amount || '}' as j, txid
             FROM postings
             WHERE txid = transactions.id
         ) v
);
--statement
INSERT INTO log(type, date, data, hash)
SELECT v.type, v.timestamp, v.data, ''
FROM (
     SELECT 0 as ord, ord as ord2, 'NEW_TRANSACTION' as type, timestamp, '{"txid": "' || id || '", "postings": ' || postings || ', "metadata": {}, "timestamp": "' || timestamp || '", "reference": "' || CASE WHEN reference IS NOT NULL THEN reference ELSE '' END || '"}' as data
     FROM transactions
     UNION ALL
     SELECT meta_id as ord, 0 as ord2, 'SET_METADATA' as type, timestamp, '{"targetType": "' || UPPER(meta_target_type) || '", "targetId": "' || meta_target_id || '", "metadata": {"' || meta_key || '": ' || CASE WHEN json_valid(meta_value) THEN meta_value ELSE '"' || meta_value || '"' END || '}}' as data
     FROM metadata
 ) v
ORDER BY v.timestamp ASC, v.ord ASC, v.ord2 ASC;
--statement
ALTER TABLE log RENAME TO log2;
--statement
-- Remove autoincrement on log table by renaming to log2, recreating the log table without the autoincrement, copy data from log2 to log, then removing log2
CREATE TABLE log
(
    "id"   integer primary key, -- without auto increment
    "type" varchar,
    "hash" varchar,
    "date" date,
    "data" varchar
);
--statement
INSERT INTO log SELECT v.id-1, v.type, v.hash, v.date, v.data FROM log2 v;
--statement
DROP TABLE log2;
--statement
UPDATE transactions
SET metadata = (
    SELECT json('{' || group_concat('"' || meta_key || '": ' || CASE WHEN json_valid(meta_value) THEN json(meta_value) ELSE '"' || meta_value || '"' END) || '}')
    FROM (
             SELECT meta_id, meta_key, meta_value
             FROM metadata
             WHERE meta_target_id = transactions.id
             GROUP BY meta_target_id, meta_key
             HAVING max(meta_id)
             ORDER BY meta_id DESC
         ) v
);
--statement
INSERT INTO accounts(address) SELECT * FROM addresses;
--statement
UPDATE accounts
SET metadata = (
    SELECT json('{' || group_concat('"' || meta_key || '":' || CASE WHEN json_valid(meta_value) THEN json(meta_value) ELSE '"' || meta_value || '"' END) || '}')
    FROM (
             SELECT meta_id, meta_key, meta_value
             FROM metadata
             WHERE meta_target_id = accounts.address
             GROUP BY meta_target_id, meta_key
             HAVING max(meta_id)
             ORDER BY meta_id DESC
         ) v
);
--statement
CREATE TRIGGER IF NOT EXISTS balances_created
    AFTER INSERT
          ON volumes
BEGIN
INSERT INTO balances('account', 'amount', 'asset') VALUES (new.account, new.input - new.output, new.asset);
END;
--statement
CREATE TRIGGER IF NOT EXISTS balances_updated
    AFTER UPDATE
              ON volumes
BEGIN
UPDATE balances SET amount = new.input - new.output WHERE account = new.account AND asset = new.asset;
END;
--statement
CREATE TRIGGER IF NOT EXISTS new_transaction
    AFTER INSERT
          ON transactions
BEGIN
INSERT OR IGNORE INTO accounts(address, metadata)
SELECT json_extract(p.value, '$.source'), '{}'
FROM json_each(new.postings) p;

INSERT OR IGNORE INTO accounts(address, metadata)
SELECT json_extract(p.value, '$.destination'), '{}'
FROM json_each(new.postings) p;

INSERT INTO volumes (account, asset, input, output)
SELECT json_extract(p.value, '$.source'),
       json_extract(p.value, '$.asset'),
       0,
       json_extract(p.value, '$.amount')
FROM json_each(new.postings) p
WHERE true
ON CONFLICT (account, asset) DO UPDATE SET output = output + excluded.output;

INSERT INTO volumes (account, asset, input, output)
SELECT json_extract(p.value, '$.destination'),
       json_extract(p.value, '$.asset'),
       json_extract(p.value, '$.amount'),
       0
FROM json_each(new.postings) p
WHERE true
ON CONFLICT (account, asset) DO UPDATE SET input = input + excluded.input;
END;
--statement
CREATE TRIGGER IF NOT EXISTS new_log_transaction
    AFTER INSERT
          ON log
              WHEN new.type = 'NEW_TRANSACTION'
BEGIN
INSERT INTO transactions (id, reference, timestamp, postings, metadata, ord)
VALUES (json_extract(new.data, '$.txid'),
        CASE
            WHEN json_extract(new.data, '$.reference') = '' THEN NULL
            ELSE json_extract(new.data, '$.reference') END,
        json_extract(new.data, '$.timestamp'),
        json_extract(new.data, '$.postings'),
        CASE
            WHEN json_extract(new.data, '$.metadata') IS NULL THEN '{}'
            ELSE json_extract(new.data, '$.metadata') END,
        (SELECT COUNT(*)+1 FROM transactions));
END;
--statement
CREATE TRIGGER IF NOT EXISTS new_log_set_metadata_on_transaction
    AFTER INSERT
          ON log
              WHEN new.type = 'SET_METADATA' AND json_extract(new.data, '$.targetType') = 'TRANSACTION'
BEGIN
UPDATE transactions
SET metadata = json_patch(metadata, json_extract(new.data, '$.metadata'))
WHERE id = json_extract(new.data, '$.targetId');
END;
--statement
CREATE TRIGGER IF NOT EXISTS new_log_set_metadata_on_account
    AFTER INSERT
          ON log
              WHEN new.type = 'SET_METADATA' AND json_extract(new.data, '$.targetType') = 'ACCOUNT'
BEGIN
INSERT INTO accounts(address, metadata)
VALUES (json_extract(new.data, '$.targetId'), json_extract(new.data, '$.metadata'))
ON CONFLICT (address) DO UPDATE SET metadata = json_patch(metadata, excluded.metadata);
END;
--statement
INSERT INTO volumes (account, asset, input, output)
SELECT destination, asset, SUM(amount), 0
FROM postings
GROUP BY asset, destination;
--statement
INSERT INTO volumes (account, asset, input, output)
SELECT source, asset, 0, SUM(amount)
FROM postings
GROUP BY asset, source
ON CONFLICT (account, asset) DO UPDATE SET output = output + excluded.output;
