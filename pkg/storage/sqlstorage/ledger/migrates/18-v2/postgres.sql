--statement
create table if not exists "VAR_LEDGER_NAME".logs_ingestion (
    onerow_id boolean PRIMARY KEY DEFAULT TRUE,
    log_id bigint
);
--statement
alter table "VAR_LEDGER_NAME".log
add column reference varchar null;
