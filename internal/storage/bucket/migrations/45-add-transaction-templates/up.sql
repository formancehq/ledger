do $$
	begin
		set search_path = '{{ .Schema }}';

		alter table schemas
		add column transactions jsonb not null default '{}'::jsonb;

		alter table transactions
		add column template text;
	end
$$;
