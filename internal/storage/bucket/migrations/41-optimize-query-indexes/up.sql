set search_path = '{{.Schema}}';

-- ========================================
-- MOVES TABLE INDEXES OPTIMIZATION
-- ========================================

-- Step 1: Create new optimized indexes FIRST to avoid performance degradation
-- Critical: Index for Point-in-Time queries with insertion_date
-- Covers queries in resource_aggregated_balances.go and resource_accounts.go
-- Replaces: moves_post_commit_volumes
create index {{ if not .Transactional }}concurrently{{end}} idx_moves_pit_insertion
    on "{{.Schema}}".moves (account_address, asset, insertion_date desc, seq desc);

-- Critical: Index for Point-in-Time queries with effective_date
-- Covers queries in resource_aggregated_balances.go and resource_accounts.go
-- Replaces: moves_effective_post_commit_volumes
create index {{ if not .Transactional }}concurrently{{end}} idx_moves_pit_effective
    on "{{.Schema}}".moves (account_address, asset, effective_date desc, seq desc);

-- Optimal: Index for balance lookups by account with effective date
-- Covers balance filtering queries in resource_accounts.go
create index {{ if not .Transactional }}concurrently{{end}} idx_moves_account_balance
    on "{{.Schema}}".moves (account_address, effective_date desc, seq desc)
    include (asset, post_commit_effective_volumes);

-- Step 2: Drop old suboptimal indexes AFTER new ones are ready
drop index if exists moves_post_commit_volumes;
drop index if exists moves_effective_post_commit_volumes;

-- ========================================
-- ACCOUNTS_METADATA TABLE OPTIMIZATION
-- ========================================

-- Step 1: Create new optimized index FIRST
-- Critical: Index for historical metadata queries
-- Covers queries in resource_accounts.go for Point-in-Time metadata
-- Replaces: accounts_metadata_revisions
create index {{ if not .Transactional }}concurrently{{end}} idx_accounts_metadata_pit
    on "{{.Schema}}".accounts_metadata (accounts_address, revision desc)
    include (metadata, date);

-- Step 2: Drop old suboptimal index AFTER new one is ready
drop index if exists accounts_metadata_revisions;

-- ========================================
-- TRANSACTIONS_METADATA TABLE OPTIMIZATION
-- ========================================

-- Step 1: Create new optimized index FIRST
-- Critical: Index for historical transaction metadata queries
-- Covers queries in resource_transactions.go for Point-in-Time metadata
-- Replaces: transactions_metadata_revisions
create index {{ if not .Transactional }}concurrently{{end}} idx_transactions_metadata_pit
    on "{{.Schema}}".transactions_metadata (transactions_id, revision desc)
    include (metadata, date);

-- Step 2: Drop old suboptimal index AFTER new one is ready
drop index if exists transactions_metadata_revisions;

-- ========================================
-- ACCOUNTS_VOLUMES TABLE CLEANUP
-- ========================================

-- Drop redundant index - PRIMARY KEY already covers this pattern
-- Safe to drop immediately as PRIMARY KEY provides same functionality
drop index if exists accounts_volumes_idx;
