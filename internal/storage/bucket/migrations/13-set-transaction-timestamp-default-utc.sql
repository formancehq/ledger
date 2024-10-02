alter table "{{.Bucket}}".transactions
add column inserted_at timestamp without time zone
default (now() at time zone 'utc');