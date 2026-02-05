do $$
	begin
		set search_path = '{{ .Schema }}';

		alter table schemas
		add column queries jsonb not null DEFAULT '{}'::jsonb;
	end
$$;
