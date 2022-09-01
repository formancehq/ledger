--statement
CREATE TABLE IF NOT EXISTS idempotency (
   "key" varchar,
   "date" varchar,
   "statusCode" int,
   "headers" varchar,
   "body" varchar,
   "requestHash" varchar,

   PRIMARY KEY("key")
);
