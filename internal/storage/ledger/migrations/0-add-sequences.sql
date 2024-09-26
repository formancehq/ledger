
-- create a sequence for transactions by ledger instead of a sequence of the table as we want to have contiguous ids
-- notes: we can still have "holes" on ids since a sql transaction can be reverted after a usage of the sequence
create sequence "{{.Bucket}}"."transaction_id_{{.ID}}" owned by "{{.Bucket}}".transactions.id;
select setval('"{{.Bucket}}"."transaction_id_{{.ID}}"', coalesce((
    select max(id) + 1
    from "{{.Bucket}}".transactions
    where ledger = '{{ .Name }}'
), 1)::bigint, false);

-- create a sequence for logs by ledger instead of a sequence of the table as we want to have contiguous ids
-- notes: we can still have "holes" on id since a sql transaction can be reverted after a usage of the sequence
create sequence "{{.Bucket}}"."log_id_{{.ID}}" owned by "{{.Bucket}}".logs.id;
select setval('"{{.Bucket}}"."log_id_{{.ID}}"', coalesce((
    select max(id) + 1
    from "{{.Bucket}}".logs
    where ledger = '{{ .Name }}'
), 1)::bigint, false);

-- enable post commit effective volumes synchronously

{{ if .HasFeature "MOVES_HISTORY_POST_COMMIT_EFFECTIVE_VOLUMES" "SYNC" }}
create index "pcev_{{.ID}}" on "{{.Bucket}}".moves (accounts_address, asset, effective_date desc) where ledger = '{{.Name}}';

create trigger "set_effective_volumes_{{.ID}}"
before insert
on "{{.Bucket}}"."moves"
for each row
when (
    new.ledger = '{{.Name}}'
)
execute procedure "{{.Bucket}}".set_effective_volumes();

create trigger "update_effective_volumes_{{.ID}}"
after insert
on "{{.Bucket}}"."moves"
for each row
when (
    new.ledger = '{{.Name}}'
)
execute procedure "{{.Bucket}}".update_effective_volumes();
{{ end }}

-- logs hash

{{ if .HasFeature "HASH_LOGS" "SYNC" }}
create trigger "set_log_hash_{{.ID}}"
before insert
on "{{.Bucket}}"."logs"
for each row
when (
    new.ledger = '{{.Name}}'
)
execute procedure "{{.Bucket}}".set_log_hash();
{{ end }}

{{ if .HasFeature "ACCOUNT_METADATA_HISTORY" "SYNC" }}
create trigger "update_account_metadata_history_{{.ID}}"
after update
on "{{.Bucket}}"."accounts"
for each row
when (
    new.ledger = '{{.Name}}'
)
execute procedure "{{.Bucket}}".update_account_metadata_history();

create trigger "insert_account_metadata_history_{{.ID}}"
after insert
on "{{.Bucket}}"."accounts"
for each row
when (
    new.ledger = '{{.Name}}'
)
execute procedure "{{.Bucket}}".insert_account_metadata_history();
{{ end }}

{{ if .HasFeature "TRANSACTION_METADATA_HISTORY" "SYNC" }}
create trigger "update_transaction_metadata_history_{{.ID}}"
after update
on "{{.Bucket}}"."transactions"
for each row
when (
    new.ledger = '{{.Name}}'
)
execute procedure "{{.Bucket}}".update_transaction_metadata_history();

create trigger "insert_transaction_metadata_history_{{.ID}}"
after insert
on "{{.Bucket}}"."transactions"
for each row
when (
    new.ledger = '{{.Name}}'
)
execute procedure "{{.Bucket}}".insert_transaction_metadata_history();
{{ end }}

{{ if .HasFeature "INDEX_ADDRESS_SEGMENTS" "ON" }}
create index "moves_accounts_address_array_{{.ID}}" on "{{.Bucket}}".moves using gin (accounts_address_array jsonb_ops) where ledger = '{{.Name}}';
create index "moves_accounts_address_array_length_{{.ID}}" on "{{.Bucket}}".moves (jsonb_array_length(accounts_address_array)) where ledger = '{{.Name}}';

create index "accounts_address_array_{{.ID}}" on "{{.Bucket}}".accounts using gin (address_array jsonb_ops) where ledger = '{{.Name}}';
create index "accounts_address_array_length_{{.ID}}" on "{{.Bucket}}".accounts (jsonb_array_length(address_array)) where ledger = '{{.Name}}';

{{ if .HasFeature "INDEX_TRANSACTION_ACCOUNTS" "ON" }}
create index "transactions_sources_arrays_{{.ID}}" on "{{.Bucket}}".transactions using gin (sources_arrays jsonb_path_ops) where ledger = '{{.Name}}';
create index "transactions_destinations_arrays_{{.ID}}" on "{{.Bucket}}".transactions using gin (destinations_arrays jsonb_path_ops) where ledger = '{{.Name}}';
{{ end }}
{{ end }}

{{ if .HasFeature "INDEX_TRANSACTION_ACCOUNTS" "ON" }}
create index "transactions_sources_{{.ID}}" on "{{.Bucket}}".transactions using gin (sources jsonb_path_ops) where ledger = '{{.Name}}';
create index "transactions_destinations_{{.ID}}" on "{{.Bucket}}".transactions using gin (destinations jsonb_path_ops) where ledger = '{{.Name}}';
{{ end }}