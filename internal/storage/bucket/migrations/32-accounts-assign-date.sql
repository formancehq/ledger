alter table "{{.Bucket}}".accounts
alter column first_usage set default (now() at time zone 'utc'),
alter column insertion_date set default (now() at time zone 'utc'),
alter column updated_at set default (now() at time zone 'utc')
;