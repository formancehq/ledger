set search_path = '{{.Bucket}}';

create index logs_idempotency_key on logs (idempotency_key);