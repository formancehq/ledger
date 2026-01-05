create index {{ if not .Transactional }}concurrently{{end}} transactions_metadata_idx on "{{.Schema}}".transactions_metadata (ledger, transactions_id);
