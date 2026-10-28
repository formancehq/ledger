create or replace function set_log_hash()
	returns trigger
	security definer
	language plpgsql
as
$$
declare
	previousHash bytea;
	marshalledAsJSON varchar;
begin
	-- Keep ledger in the sort keys: with an equality, PostgreSQL can remove
	-- it and scan logs_ids backwards, filtering other ledgers' higher IDs.
	-- Equal range bounds retain the same rows while allowing (ledger, id)
	-- to supply the requested order directly, including in custom plans.
	select hash into previousHash
	from logs
	where ledger >= new.ledger and ledger <= new.ledger
	order by ledger desc, id desc
	limit 1;

	-- select only fields participating in the hash on the backend and format json representation the same way
	select '{' ||
		'"type":"' || new.type || '",' ||
		'"data":' || encode(new.memento, 'escape') || ',' ||
		'"date":"' || (to_json(new.date::timestamp)#>>'{}') || 'Z",' ||
		'"idempotencyKey":"' || coalesce(new.idempotency_key, '') || '",' ||
		'"id":0,' ||
		'"hash":null' ||
   '}' into marshalledAsJSON;

	new.hash = (
		select public.digest(
			case
			when previousHash is null
			then marshalledAsJSON::bytea
			else '"' || encode(previousHash::bytea, 'base64')::bytea || E'"\n' || marshalledAsJSON::bytea
			end || E'\n', 'sha256'::text
		)
	);

	return new;
end;
$$ set search_path = '{{ .Schema }}';

