set search_path = '{{.Schema}}';

-- ========================================
-- MOVES TABLE INDEXES OPTIMIZATION (1/4)
-- ========================================

-- Critical: Index for Point-in-Time queries with insertion_date
-- Covers queries in resource_aggregated_balances.go and resource_accounts.go
-- Replaces: moves_post_commit_volumes
create index {{ if not .Transactional }}concurrently{{end}} idx_moves_pit_insertion
    on "{{.Schema}}".moves (account_address, asset, insertion_date desc, seq desc);
