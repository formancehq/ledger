alter table "{{.Bucket}}".logs
alter column hash
drop not null;