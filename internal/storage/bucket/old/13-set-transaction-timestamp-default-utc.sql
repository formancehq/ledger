alter table transactions
add column inserted_at timestamp without time zone
default (now() at time zone 'utc');

alter table transactions
alter column timestamp set default (now() at time zone 'utc');