CREATE OR REPLACE FUNCTION "VAR_LEDGER_NAME".meta_compare(metadata jsonb, value varchar, variadic path TEXT[])
    RETURNS BOOLEAN
AS
$$
BEGIN
    return jsonb_extract_path_text(metadata, variadic path)::varchar = value::varchar;
EXCEPTION
    WHEN others THEN
        RAISE INFO 'Error Name: %', SQLERRM;
        RAISE INFO 'Error State: %', SQLSTATE;
        RETURN false;
END
$$
    LANGUAGE plpgsql
    IMMUTABLE;
--statement
CREATE OR REPLACE FUNCTION "VAR_LEDGER_NAME".meta_compare(metadata jsonb, value bool, variadic path TEXT[])
    RETURNS BOOLEAN
AS
$$
BEGIN
    return jsonb_extract_path(metadata, variadic path)::bool = value::bool;
EXCEPTION
    WHEN others THEN
        RAISE INFO 'Error Name: %', SQLERRM;
        RAISE INFO 'Error State: %', SQLSTATE;
        RETURN false;
END
$$
    LANGUAGE plpgsql
    IMMUTABLE;
--statement
CREATE OR REPLACE FUNCTION "VAR_LEDGER_NAME".meta_compare(metadata jsonb, value numeric, variadic path TEXT[])
    RETURNS BOOLEAN
AS
$$
BEGIN
    return jsonb_extract_path(metadata, variadic path)::numeric = value::numeric;
EXCEPTION
    WHEN others THEN
        RAISE INFO 'Error Name: %', SQLERRM;
        RAISE INFO 'Error State: %', SQLSTATE;
        RETURN false;
END
$$
    LANGUAGE plpgsql
    IMMUTABLE;
