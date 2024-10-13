alter table "{{.Bucket}}".moves
alter column insertion_date set default (now() at time zone 'utc'),
alter column effective_date set default (now() at time zone 'utc')
;