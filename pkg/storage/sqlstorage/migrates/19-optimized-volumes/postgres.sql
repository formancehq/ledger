--statement
alter table "VAR_LEDGER_NAME".volumes add column if not exists account_json jsonb;
--statement
UPDATE "VAR_LEDGER_NAME".volumes SET account_json = to_jsonb(string_to_array(account, ':'));
--statement
create index if not exists volumes_account_json on "VAR_LEDGER_NAME".volumes using GIN(account_json);