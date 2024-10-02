create function "{{.Bucket}}".set_effective_volumes()
    returns trigger
    security definer
    language plpgsql
as
$$
begin
    new.post_commit_effective_volumes = coalesce((
        select json_build_object(
            'input', (post_commit_effective_volumes->>'input')::numeric + case when new.is_source then 0 else new.amount end,
            'output', (post_commit_effective_volumes->>'output')::numeric + case when new.is_source then new.amount else 0 end
        )
        from "{{.Bucket}}".moves
        where accounts_address = new.accounts_address
            and asset = new.asset
            and ledger = new.ledger
            and (effective_date < new.effective_date or (effective_date = new.effective_date and seq < new.seq))
        order by effective_date desc, seq desc
        limit 1
    ), json_build_object(
        'input', case when new.is_source then 0 else new.amount end,
        'output', case when new.is_source then new.amount else 0 end
    ));

    return new;
end;
$$;

create function "{{.Bucket}}".update_effective_volumes()
    returns trigger
    security definer
    language plpgsql
as
$$
begin
    update "{{.Bucket}}".moves
    set post_commit_effective_volumes = json_build_object(
		'input', (post_commit_effective_volumes->>'input')::numeric + case when new.is_source then 0 else new.amount end,
		'output', (post_commit_effective_volumes->>'output')::numeric + case when new.is_source then new.amount else 0 end
    )
    where accounts_address = new.accounts_address
        and asset = new.asset
        and effective_date > new.effective_date
        and ledger = new.ledger;

    return new;
end;
$$;

create or replace function "{{.Bucket}}".update_transaction_metadata_history() returns trigger
	security definer
	language plpgsql
as
$$
begin
	insert into "{{.Bucket}}".transactions_metadata (ledger, transactions_id, revision, date, metadata)
	values (new.ledger, new.id, (
		select revision + 1
		from "{{.Bucket}}".transactions_metadata
		where transactions_metadata.transactions_id = new.id and transactions_metadata.ledger = new.ledger
		order by revision desc
		limit 1
	), new.updated_at, new.metadata);

	return new;
end;
$$;

create or replace function "{{.Bucket}}".insert_transaction_metadata_history() returns trigger
	security definer
	language plpgsql
as
$$
begin
	insert into "{{.Bucket}}".transactions_metadata (ledger, transactions_id, revision, date, metadata)
	values (new.ledger, new.id, 1, new.timestamp, new.metadata);

	return new;
end;
$$;

create or replace function "{{.Bucket}}".update_account_metadata_history() returns trigger
	security definer
	language plpgsql
as
$$
begin
	insert into "{{.Bucket}}".accounts_metadata (ledger, accounts_address, revision, date, metadata)
	values (new.ledger, new.address, (
		select revision + 1
		from "{{.Bucket}}".accounts_metadata
		where accounts_metadata.accounts_address = new.address
		order by revision desc
		limit 1
	), new.updated_at, new.metadata);

	return new;
end;
$$;

create or replace function "{{.Bucket}}".insert_account_metadata_history() returns trigger
	security definer
	language plpgsql
as
$$
begin
	insert into "{{.Bucket}}".accounts_metadata (ledger, accounts_address, revision, date, metadata)
	values (new.ledger, new.address, 1, new.insertion_date, new.metadata);

	return new;
end;
$$;

create or replace function "{{.Bucket}}".explode_address(_address varchar)
	returns jsonb
	language sql
	immutable
as
$$
select public.aggregate_objects(jsonb_build_object(data.number - 1, data.value))
from (select row_number() over () as number, v.value
      from (select unnest(string_to_array(_address, ':')) as value
            union all
            select null) v) data
$$;

create or replace function "{{.Bucket}}".set_transaction_addresses() returns trigger
	security definer
	language plpgsql
as
$$
begin

	new.sources = (
		select to_jsonb(array_agg(v->>'source')) as value
		from jsonb_array_elements(new.postings::jsonb) v
	);
	new.destinations = (
		select to_jsonb(array_agg(v->>'destination')) as value
		from jsonb_array_elements(new.postings::jsonb) v
	);

	return new;
end
$$;

create or replace function "{{.Bucket}}".set_transaction_addresses_segments() returns trigger
	security definer
	language plpgsql
as
$$
begin
	new.sources_arrays = (
		select to_jsonb(array_agg("{{.Bucket}}".explode_address(v ->> 'source'))) as value
		from jsonb_array_elements(new.postings::jsonb) v
	);
	new.destinations_arrays = (
		select to_jsonb(array_agg("{{.Bucket}}".explode_address(v ->> 'destination'))) as value
		from jsonb_array_elements(new.postings::jsonb) v
	);

	return new;
end
$$;

create or replace function "{{.Bucket}}".set_address_array_for_account() returns trigger
	security definer
	language plpgsql
as
$$
begin
	new.address_array = to_json(string_to_array(new.address, ':'));

	return new;
end
$$;
--
-- --todo: add test for changing logs format of created transaction and reverted transaction