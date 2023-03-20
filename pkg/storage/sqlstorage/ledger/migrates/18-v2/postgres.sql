--statement
create table if not exists "VAR_LEDGER_NAME".logs_ingestion (
    log_id bigint primary key
);
--statement
alter table "VAR_LEDGER_NAME".log
add column reference varchar null;
