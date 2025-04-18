do $$
	begin
		set search_path = '{{.Schema}}';
		assert (select count(*) from transactions where updated_at is null) = 0, 'updated_at should not be null';
	end;
$$