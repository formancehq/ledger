drop index logs_idempotency_key;

create unique index logs_idempotency_key on logs (ledger, idempotency_key);