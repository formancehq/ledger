do $$
	declare
		_offset integer := 0;
		_batch_size integer := 1000;
	begin
		set search_path = '{{ .Schema }}';

		if (select count(*) from logs) = 0 then
			return;
		end if;

		perform pg_notify('migrations-{{ .Schema }}', 'init: ' || (select count(*) from logs));

		loop
			with data as (
				select *
				from logs
				order by seq
				offset _offset
				limit _batch_size
			)
			update logs
			set memento = convert_to(
				replace(
					replace(
						replace(
							case
								when logs.type = 'NEW_TRANSACTION' then public.json_compact(json_build_object(
								    'transaction', json_strip_nulls(json_build_object(
								        'postings', (
								            select to_json(array_agg(public.json_compact(json_build_object('source', v->>'source', 'destination', v->>'destination', 'amount', (v->>'amount')::numeric, 'asset', v->>'asset'))))
								            from json_array_elements((logs.data->'transaction'->'postings')::json) v
								        ),
								        'metadata', (
								            select json_object_agg(key, value order by key)
								            from json_each_text((logs.data->'transaction'->'metadata')::json)
								        ),
								        'timestamp', logs.data->'transaction'->'timestamp',
								        'reference', logs.data->'transaction'->'reference',
								        'id', logs.data->'transaction'->'id',
								        'reverted', false
								    )),
								    'accountMetadata', coalesce((
									    select json_object_agg(key, (
										    select json_object_agg(key, value order by key)
										    from json_each_text(value)
									    ) order by key)
									    from json_each((logs.data->'accountMetadata')::json)
								    ), '{}'::json)
								))::varchar
								when logs.type = 'SET_METADATA' then public.json_compact(json_build_object(
								    'targetType', logs.data->>'targetType',
								    'targetId', (logs.data->>'targetId')::json,
								    'metadata', (
								        select json_object_agg(key, value order by key)
								        from json_each_text((logs.data->'metadata')::json)
								    )
								))::varchar
								when logs.type = 'REVERTED_TRANSACTION' then public.json_compact(json_build_object(
								    'revertedTransactionID', (logs.data->>'revertedTransactionID')::numeric,
								    'transaction', json_strip_nulls(json_build_object(
								        'postings', (
								            select to_json(array_agg(public.json_compact(json_build_object('source', v->>'source', 'destination', v->>'destination', 'amount', (v->>'amount')::numeric, 'asset', v->>'asset'))))
								            from json_array_elements((logs.data->'transaction'->'postings')::json) v
								        ),
								        'metadata', (
								            select json_object_agg(key, value order by key)
								            from json_each_text((logs.data->'transaction'->'metadata')::json)
								        ),
								        'timestamp', logs.data->'transaction'->'timestamp',
								        'reference', logs.data->'transaction'->'reference',
								        'id', logs.data->'transaction'->'id',
								        'reverted', false
								    ))
								))::varchar
								else convert_from(logs.memento, 'utf-8')
							end
	                   , '&'::varchar, '\u0026'::varchar)
	                , '<'::varchar, '\u003c'::varchar)
	            , '>'::varchar, '\u003e'::varchar
			), 'utf-8')
			from data
			where logs.seq = data.seq;

			exit when not found;

			_offset = _offset + _batch_size;

			perform pg_notify('migrations-{{ .Schema }}', 'continue: ' || _batch_size);

			commit;
		end loop;

		drop table if exists txs_view;
	end
$$;

