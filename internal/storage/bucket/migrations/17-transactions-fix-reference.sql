drop index transactions_reference;
create unique index transactions_reference on transactions (ledger, reference);