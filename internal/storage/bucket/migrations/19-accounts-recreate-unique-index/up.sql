-- There is already a covering index on accounts table (including seq column).
-- As we will remove the seq column in next migration, we have to create a new index without it (PG will remove it automatically in background).
-- Also, we create the index concurrently to avoid locking the table.
-- And, as there is already an index on this table, the index creation should not fail.
--
-- We create this index in a dedicated as, as the doc mentions it (https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-MULTI-STATEMENT)
-- multi statements queries are automatically wrapped inside transaction block, and it's forbidden
-- to create index concurrently inside a transaction block.
create unique index concurrently accounts_ledger2 on "{{.Bucket}}".accounts (ledger, address)