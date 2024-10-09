alter table "{{.Bucket}}".logs
add column idempotency_hash bytea;