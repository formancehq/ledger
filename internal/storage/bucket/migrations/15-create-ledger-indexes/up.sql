set search_path = '{{.Schema}}';

drop trigger enforce_reference_uniqueness on transactions;
drop function enforce_reference_uniqueness();

drop index transactions_reference;
alter index transactions_reference2 rename to transactions_reference;