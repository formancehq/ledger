alter table logs
add column memento bytea;

update logs
set memento = convert_to(data::varchar, 'LATIN1')::bytea;

alter table logs
alter column memento set not null;