--todo: add special traitement on code when value is empty
alter table "{{.Bucket}}".logs
add column idempotency_hash bytea;