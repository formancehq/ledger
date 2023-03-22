--statement
create table if not exists "VAR_LEDGER_NAME".logs_ingestion (
    onerow_id boolean PRIMARY KEY DEFAULT TRUE,
    log_id bigint
);

--statement
alter table "VAR_LEDGER_NAME".log
add column reference varchar null;

--statement
create table if not exists "VAR_LEDGER_NAME".logs_v2 (
    "id"   bigint,
    "type" smallint,
	"hash" varchar(256),
    "date" timestamp with time zone,
    "data" bytea,
    "reference" text,
	UNIQUE ("id")
);
