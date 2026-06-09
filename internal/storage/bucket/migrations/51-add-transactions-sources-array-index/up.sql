create index {{ if not .Transactional }}concurrently{{end}} if not exists transactions_sources_arrays_gin
on "{{.Schema}}".transactions using gin (sources_arrays jsonb_path_ops);
