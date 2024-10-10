--todo: add migration
alter table "{{.Bucket}}".logs
add column memento bytea;