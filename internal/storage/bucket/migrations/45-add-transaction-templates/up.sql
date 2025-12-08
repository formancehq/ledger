do $$
	begin
		set search_path = '{{ .Schema }}';

		alter table transactions
		add column template text;
	end
$$;
