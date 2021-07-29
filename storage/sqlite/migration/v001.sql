--statement
CREATE TABLE IF NOT EXISTS transactions (
  "id"        integer,
  "timestamp" varchar,
  "reference" varchar,
  "hash"      varchar,

  UNIQUE("id"),
  UNIQUE("reference")
);
--statement
CREATE TABLE IF NOT EXISTS postings (
  "id"          integer,
  "txid"        integer,
  "source"      varchar,
  "destination" varchar,
  "amount"      integer,
  "asset"       varchar,

  UNIQUE("id", "txid")
);
--statement
CREATE INDEX IF NOT EXISTS 'p_c0' ON "postings" (
  "txid" DESC,
  "source",
  "destination"
);
--statement
CREATE TABLE IF NOT EXISTS metadata (
  "meta_id"          integer,
  "meta_target_type" varchar,
  "meta_target_id"   varchar,
  "meta_key"         varchar,
  "meta_type"        varchar,
  "meta_value"       varchar,
  "timestamp"        varchar,

  UNIQUE("meta_id")
);
--statement
CREATE INDEX IF NOT EXISTS 'm_i0' ON "metadata" (
  "meta_target_type",
  "meta_target_id"
);
--statement
CREATE VIEW IF NOT EXISTS addresses AS SELECT address FROM (
  SELECT source as address FROM postings GROUP BY source
  UNION
  SELECT destination as address FROM postings GROUP BY destination
) GROUP BY address;