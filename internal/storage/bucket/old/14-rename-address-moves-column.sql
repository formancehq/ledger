--todo: must be transactional
-- to be transparent, the table which list migrations must be updated in the same transaction as this change
alter table moves
rename column account_address to accounts_address;

-- todo: column removed later, we don't need to change its type
alter table moves
rename column account_address_array to accounts_address_array;