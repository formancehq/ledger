create index {{ if not .Transactional }}concurrently{{end}} if not exists transactions_destinations_arrays_gin
on "{{.Schema}}".transactions using gin (destinations_arrays jsonb_path_ops);
