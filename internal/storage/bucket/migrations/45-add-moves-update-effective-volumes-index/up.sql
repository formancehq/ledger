set search_path = '{{.Schema}}';

-- ========================================
-- MOVES TABLE INDEX FOR TRIGGER OPTIMIZATION (3/5)
-- ========================================

-- Critical: Index to optimize update_effective_volumes trigger
-- This trigger runs on EVERY INSERT when MOVES_HISTORY feature is ON
-- The trigger updates all moves with effective_date > new.effective_date
-- Replaces/optimizes: moves_range_dates which has suboptimal column order
create index idx_moves_update_effective_volumes
    on "{{.Schema}}".moves (accounts_address, asset, effective_date);
