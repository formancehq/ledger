drop index "{{.Bucket}}".logs_idempotency_key;

create unique index logs_idempotency_key on "{{.Bucket}}".logs (ledger, idempotency_key);