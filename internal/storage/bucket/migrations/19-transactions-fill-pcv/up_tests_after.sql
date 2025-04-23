do $$
	declare
		expected varchar = '{"fees": {"USD": {"input": 1, "output": 0}}, "world": {"USD": {"input": 0, "output": 100}, "SELL": {"input": 0, "output": 1}}, "orders:0": {"USD": {"input": 100, "output": 100}}, "sellers:0": {"USD": {"input": 99, "output": 0}, "SELL": {"input": 1, "output": 0}}}';
	begin
		set search_path = '{{.Schema}}';
		assert (select post_commit_volumes::varchar from transactions where id = 0) = expected,
			'post_commit_volumes should be equals to ' || expected || ' but was ' || (select to_jsonb(post_commit_volumes) from transactions where id = 0);
	end;
$$