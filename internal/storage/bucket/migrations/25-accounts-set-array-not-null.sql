alter table "{{.Bucket}}".accounts
alter column address_array drop not null;