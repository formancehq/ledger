set search_path = '{{.Schema}}';

create index transactions_reference on transactions (reference);