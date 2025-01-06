-- todo: clean empty reference in subsequent migration
create unique index concurrently transactions_reference2 on "{{.Schema}}".transactions (ledger, reference) where reference <> '';