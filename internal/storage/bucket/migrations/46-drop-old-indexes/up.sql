set search_path = '{{.Schema}}';

-- ========================================
-- DROP OLD INDEXES
-- ========================================

-- Drop old moves table indexes (replaced by migrations 41-42-45)
drop index if exists moves_post_commit_volumes;
drop index if exists moves_effective_post_commit_volumes;
drop index if exists moves_range_dates;

-- Drop old accounts_metadata index (replaced by migration 43)
drop index if exists accounts_metadata_revisions;

-- Drop old transactions_metadata index (replaced by migration 44)
drop index if exists transactions_metadata_revisions;

-- Drop redundant accounts_volumes index - PRIMARY KEY already covers this pattern
drop index if exists accounts_volumes_idx;
