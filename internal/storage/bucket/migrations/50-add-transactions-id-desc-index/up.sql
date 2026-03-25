create index {{ if not .Transactional }}concurrently{{end}} transactions_id_desc on "{{.Schema}}".transactions (id desc);
