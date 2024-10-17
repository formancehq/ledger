-- drop accounts_address_array from moves
alter table "{{.Bucket}}".moves
drop column accounts_address_array;