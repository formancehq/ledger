alter table logs
alter column date set default (now() at time zone 'utc');