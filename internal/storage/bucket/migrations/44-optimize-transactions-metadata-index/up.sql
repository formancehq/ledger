set search_path = '{{.Schema}}';

-- ========================================
-- TRANSACTIONS_METADATA TABLE OPTIMIZATION
-- ========================================

-- Critical: Index for historical transaction metadata queries
-- Covers queries in resource_transactions.go for Point-in-Time metadata
-- Replaces: transactions_metadata_revisions
create index {{ if not .Transactional }}concurrently{{end}} idx_transactions_metadata_pit
    on "{{.Schema}}".transactions_metadata (transactions_id, revision desc)
    include (metadata, date);
