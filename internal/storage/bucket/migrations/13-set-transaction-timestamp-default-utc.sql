alter table "{{.Bucket}}".transactions
add column inserted_at timestamp without time zone
default (now() at time zone 'utc');

alter table "{{.Bucket}}".transactions
alter column timestamp set default (now() at time zone 'utc');