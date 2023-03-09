--statement
CREATE TABLE IF NOT EXISTS idempotency (
   "key" varchar,
   "date" varchar,
   "status_code" int,
   "headers" varchar,
   "body" varchar,
   "request_hash" varchar,

   PRIMARY KEY("key")
);
