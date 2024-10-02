alter table "{{.Bucket}}".transactions
alter column id
type bigint;

alter table "{{.Bucket}}".transactions
drop column seq;