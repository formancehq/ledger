alter table "moves"
alter column post_commit_volumes drop not null,
alter column post_commit_effective_volumes drop not null
;