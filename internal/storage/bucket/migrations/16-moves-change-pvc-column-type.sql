-- update post_commit_volumes of table moves to jsonb
alter table "{{.Bucket}}".moves
add column post_commit_volumes_jsonb jsonb;

update "{{.Bucket}}".moves
set post_commit_volumes_jsonb = json_build_object('input', (post_commit_volumes).inputs, 'output', (post_commit_volumes).outputs);

alter table "{{.Bucket}}".moves
drop column post_commit_volumes;

alter table "{{.Bucket}}".moves
rename post_commit_volumes_jsonb to post_commit_volumes;

-- update post_commit_volumes of table moves to jsonb
alter table "{{.Bucket}}".moves
add column post_commit_effective_volumes_jsonb jsonb;

update "{{.Bucket}}".moves
set post_commit_effective_volumes_jsonb = json_build_object('input', (post_commit_effective_volumes).inputs, 'output', (post_commit_effective_volumes).outputs);

alter table "{{.Bucket}}".moves
drop column post_commit_effective_volumes;

alter table "{{.Bucket}}".moves
rename post_commit_effective_volumes_jsonb to post_commit_effective_volumes;