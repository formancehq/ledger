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
    "metadata" varchar NOT NULL,

    UNIQUE ("address")
);
--statement
ALTER TABLE transactions
ADD COLUMN "postings" varchar NOT NULL;
--statement
ALTER TABLE transactions
ADD COLUMN "metadata" varchar NOT NULL;
--statement
CREATE TABLE IF NOT EXISTS log
(
    "id"   integer,
    "type" varchar,
    "hash" varchar,
    "date" date,
    "data" varchar,

    UNIQUE ("id")
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
    INSERT INTO transactions (id, reference, timestamp, postings, metadata)
    VALUES (json_extract(new.data, '$.txid'),
            CASE
                WHEN json_extract(new.data, '$.reference') = '' THEN NULL
                ELSE json_extract(new.data, '$.reference') END,
            json_extract(new.data, '$.timestamp'),
            json_extract(new.data, '$.postings'),
            CASE
                WHEN json_extract(new.data, '$.metadata') IS NULL THEN '{}'
                ELSE json_extract(new.data, '$.metadata') END);
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