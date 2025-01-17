alter table "{{.Schema}}"."accounts_volumes"
drop constraint "accounts_volumes_pkey";

-- todo: put in a dedicated migration to avoid lock the table for too long
create index "accounts_volumes_ledger_accounts_address_asset_idx"
on "{{.Schema}}"."accounts_volumes" (ledger, accounts_address, asset) include (input, output);