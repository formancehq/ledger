alter table "{{.Bucket}}".logs
add column memento bytea;

update "{{.Bucket}}".logs
set memento = convert_to(data::varchar, 'LATIN1')::bytea;

alter table "{{.Bucket}}".logs
alter column memento set not null;