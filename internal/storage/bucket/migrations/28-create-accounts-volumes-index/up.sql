create index concurrently "accounts_volumes_ledger_accounts_address_asset_idx"
on "{{.Schema}}"."accounts_volumes" (ledger, accounts_address, asset) include (input, output);