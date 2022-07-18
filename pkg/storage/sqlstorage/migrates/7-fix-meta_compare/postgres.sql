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
