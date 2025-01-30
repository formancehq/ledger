set search_path = '{{.Schema}}';

create table logs_blocks (
	id serial,
	previous bigint primary key,
	ledger varchar,
	from_id bigint,
	to_id bigint,
	hash bytea,
	date timestamp without time zone
);

create type block as (
	max_log_id bigint,
	block_id bigint,
	hash bytea
);

create or replace function create_block(_ledger varchar, max_block_size integer, previous_block block) returns block
	language plpgsql
as $$
declare
	hash bytea;
	max_log_id bigint;
	new_block_id bigint;
begin
	select max(id), public.digest(coalesce(previous_block.hash, '') || string_agg(
		type ||
		encode(memento, 'escape') ||
		(to_json(date::timestamp)#>>'{}') ||
		coalesce(idempotency_key, '') ||
		id,
		''
    ), 'sha256'::text)
	from (
		select *
		from logs
		where id > previous_block.max_log_id and ledger = _ledger
		order by id
		limit max_block_size
	) logs
	into max_log_id, hash
	;
	if max_log_id is null then
		return (0, 0, null)::block;
	end if;

	insert into logs_blocks (ledger, previous, from_id, to_id, hash, date)
	values (_ledger, previous_block.block_id, previous_block.max_log_id, max_log_id, hash, now())
	returning id
	into new_block_id;

	return (max_log_id, new_block_id, hash)::block;
end;
$$ set search_path from current;

create or replace procedure create_blocks(_ledger varchar, max_block_size integer)
	language plpgsql
as $$
declare
	previous_block block;
begin
	select to_id, id, hash
	from logs_blocks
	where ledger = _ledger
	order by previous desc
	limit 1
	into previous_block;

	if not found then
        previous_block = (0, 0, null)::block;
    end if;

	loop
		select *
        from create_block(_ledger, max_block_size, coalesce(previous_block, (0,0, null)::block)) v
		into previous_block;
		if previous_block.max_log_id = 0 then
			return;
		end if;
	end loop;
end;
$$ set search_path from current;