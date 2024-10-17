drop index "{{.Bucket}}".transactions_reference;
create unique index transactions_reference on "{{.Bucket}}".transactions (ledger, reference);