create or replace function "{{.Bucket}}".insert_move(
    _transactions_seq bigint,
    _ledger varchar,
    _insertion_date timestamp without time zone,
    _effective_date timestamp without time zone,
    _account_address varchar,
    _asset varchar,
    _amount numeric,
    _is_source bool,
    _account_exists bool)
    returns void
    language plpgsql
as
$$
declare
    _post_commit_volumes           "{{.Bucket}}".volumes = (0, 0)::"{{.Bucket}}".volumes;
    _effective_post_commit_volumes "{{.Bucket}}".volumes = (0, 0)::"{{.Bucket}}".volumes;
    _seq                           bigint;
    _account_seq                   bigint;
begin
    select seq from "{{.Bucket}}".accounts where ledger = _ledger and address = _account_address into _account_seq;

    if _account_exists then
        select (post_commit_volumes).inputs, (post_commit_volumes).outputs
        into _post_commit_volumes
        from "{{.Bucket}}".moves
        where accounts_seq = _account_seq
          and asset = _asset
        order by seq desc
        limit 1;

        if not found then
            _post_commit_volumes = (0, 0)::"{{.Bucket}}".volumes;
            _effective_post_commit_volumes = (0, 0)::"{{.Bucket}}".volumes;
        else
            select (post_commit_effective_volumes).inputs, (post_commit_effective_volumes).outputs into _effective_post_commit_volumes
            from "{{.Bucket}}".moves
            where accounts_seq = _account_seq
              and asset = _asset
              and effective_date <= _effective_date
            order by effective_date desc, seq desc
            limit 1;

            if not found then
                _effective_post_commit_volumes = (0, 0)::"{{.Bucket}}".volumes;
            end if;
        end if;
    end if;

    if _is_source then
        _post_commit_volumes.outputs = _post_commit_volumes.outputs + _amount;
        _effective_post_commit_volumes.outputs = _effective_post_commit_volumes.outputs + _amount;
    else
        _post_commit_volumes.inputs = _post_commit_volumes.inputs + _amount;
        _effective_post_commit_volumes.inputs = _effective_post_commit_volumes.inputs + _amount;
    end if;

    insert into "{{.Bucket}}".moves (ledger,
                       insertion_date,
                       effective_date,
                       accounts_seq,
                       account_address,
                       asset,
                       transactions_seq,
                       amount,
                       is_source,
                       account_address_array,
                       post_commit_volumes,
                       post_commit_effective_volumes)
    values (_ledger,
            _insertion_date,
            _effective_date,
            _account_seq,
            _account_address,
            _asset,
            _transactions_seq,
            _amount,
            _is_source,
            (select to_json(string_to_array(_account_address, ':'))),
            _post_commit_volumes,
            _effective_post_commit_volumes)
    returning seq into _seq;

    if _account_exists then
        update "{{.Bucket}}".moves
        set post_commit_effective_volumes =
                ((post_commit_effective_volumes).inputs + case when _is_source then 0 else _amount end,
                 (post_commit_effective_volumes).outputs + case when _is_source then _amount else 0 end
                    )
        where accounts_seq = _account_seq
          and asset = _asset
          and effective_date > _effective_date;

        update "{{.Bucket}}".moves
        set post_commit_effective_volumes =
                ((post_commit_effective_volumes).inputs + case when _is_source then 0 else _amount end,
                 (post_commit_effective_volumes).outputs + case when _is_source then _amount else 0 end
                    )
        where accounts_seq = _account_seq
          and asset = _asset
          and effective_date = _effective_date
          and seq < _seq;
    end if;
end;
$$;