alter table "{{.Bucket}}".moves
rename column account_address to accounts_address;

alter table "{{.Bucket}}".moves
rename column account_address_array to accounts_address_array;