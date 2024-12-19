create index concurrently accounts_volumes_idx on "{{.Schema}}".accounts_volumes (ledger, accounts_address, asset) include (input, output);
