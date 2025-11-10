set search_path = '{{.Schema}}';

-- ========================================
-- MOVES TABLE INDEXES OPTIMIZATION (2/4)
-- ========================================

-- Critical: Index for Point-in-Time queries with effective_date
-- Covers queries in resource_aggregated_balances.go and resource_accounts.go
-- Replaces: moves_effective_post_commit_volumes
create index idx_moves_pit_effective
    on "{{.Schema}}".moves (account_address, asset, effective_date desc, seq desc);
