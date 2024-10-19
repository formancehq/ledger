set search_path = '{{.Schema}}';

update logs
set idempotency_key = null
where idempotency_key = '';

update logs
set idempotency_key = null
where id in (
    select unnest(duplicateLogIds.ids[2:]) as id
    from (
        select array_agg(id order by id) as ids
        from logs l
        where idempotency_key is not null
        group by idempotency_key
        having count(*) > 1
    ) duplicateLogIds
);

drop index logs_idempotency_key;

create unique index logs_idempotency_key on logs (idempotency_key);