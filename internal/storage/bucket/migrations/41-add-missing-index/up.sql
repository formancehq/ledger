create index {{ if not .Transactional }}concurrently{{end}} logs_ids on "{{.Schema}}".logs (id);
