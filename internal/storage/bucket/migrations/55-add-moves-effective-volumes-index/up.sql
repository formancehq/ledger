-- set_effective_volumes looks up an account's previous move ordered by (effective_date, seq). Without seq in an index,
-- every earlier move of the account at the same effective date is read and sorted, which is quadratic per transaction.
create index {{ if not .Transactional }}concurrently{{end}} if not exists moves_effective_volumes
on "{{.Schema}}".moves (ledger, accounts_address, asset, effective_date desc, seq desc);
