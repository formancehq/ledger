-- Two migrations share the 51 prefix and are ordered by position. A
-- bucket migrated with v2.4.10 already recorded the position that the
-- logs_blocks fix took in v2.4.11, so the fix never ran on it. Apply it
-- again only where the primary key is not already (ledger, previous).
do $$
	declare
		pkey_name name;
		pkey_columns text[];
	begin
		set search_path = '{{ .Schema }}';

		select c.conname, array_agg(a.attname::text order by k.ord)
		into pkey_name, pkey_columns
		from pg_constraint c
		cross join lateral unnest(c.conkey) with ordinality as k(attnum, ord)
		join pg_attribute a on a.attrelid = c.conrelid and a.attnum = k.attnum
		where c.conrelid = 'logs_blocks'::regclass and c.contype = 'p'
		group by c.conname;

		if pkey_columns is not distinct from array['ledger', 'previous'] then
			return;
		end if;

		if pkey_name is not null then
			execute format('alter table logs_blocks drop constraint %I', pkey_name);
		end if;

		alter table logs_blocks add primary key (ledger, previous);
	end
$$;
