set search_path = '{{.Bucket}}';

create index transactions_reference on transactions (reference);