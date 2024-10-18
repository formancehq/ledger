create unique index accounts_metadata_ledger on accounts_metadata (ledger, accounts_address, revision);
create index accounts_metadata_revisions on accounts_metadata(accounts_address asc, revision desc) include (metadata, date);

create unique index transactions_metadata_ledger on transactions_metadata (ledger, transactions_id, revision);
create index transactions_metadata_revisions on transactions_metadata(transactions_id asc, revision desc) include (metadata, date);

drop index transactions_sources_arrays;
drop index transactions_destinations_arrays;
drop index accounts_address_array;
drop index accounts_address_array_length;
drop index transactions_sources;
drop index transactions_destinations;
