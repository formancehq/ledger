set search_path = '{{.Schema}}';

create index logs_idempotency_key on logs (idempotency_key);