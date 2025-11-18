set search_path = '{{.Schema}}';

-- ========================================
-- ACCOUNTS_METADATA TABLE OPTIMIZATION
-- ========================================

-- Critical: Index for historical metadata queries
-- Covers queries in resource_accounts.go for Point-in-Time metadata
-- Replaces: accounts_metadata_revisions
create index {{ if not .Transactional }}concurrently{{end}} idx_accounts_metadata_pit
    on "{{.Schema}}".accounts_metadata (accounts_address, revision desc)
    include (metadata, date);
