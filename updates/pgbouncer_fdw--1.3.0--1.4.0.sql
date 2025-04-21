
--------
CREATE TEMP TABLE pgbouncer_fdw_preserve_privs_temp (statement text);

INSERT INTO pgbouncer_fdw_preserve_privs_temp
SELECT 'GRANT EXECUTE ON FUNCTION @extschema@.pgbouncer_databases_func()  TO '||array_to_string(array_agg('"'||grantee::text||'"'), ',')||';' 
FROM information_schema.routine_privileges
WHERE routine_schema = '@extschema@'
AND routine_name = 'pgbouncer_databases_func'
AND grantee != 'PUBLIC';

INSERT INTO pgbouncer_fdw_preserve_privs_temp
SELECT 'GRANT EXECUTE ON FUNCTION @extschema@.pgbouncer_users_func()  TO '||array_to_string(array_agg('"'||grantee::text||'"'), ',')||';'
FROM information_schema.routine_privileges
WHERE routine_schema = '@extschema@'
AND routine_name = 'pgbouncer_users_func'
AND grantee != 'PUBLIC';

INSERT INTO pgbouncer_fdw_preserve_privs_temp
SELECT 'GRANT '||string_agg(privilege_type, ',')||' ON @extschema@.pgbouncer_databases TO '||grantee::text||';'
FROM information_schema.table_privileges
WHERE table_schema = '@extschema@'
AND table_name = 'pgbouncer_databases'
AND grantee != 'PUBLIC'
GROUP BY grantee;

INSERT INTO pgbouncer_fdw_preserve_privs_temp
SELECT 'GRANT '||string_agg(privilege_type, ',')||' ON @extschema@.pgbouncer_users TO '||grantee::text||';'
FROM information_schema.table_privileges
WHERE table_schema = '@extschema@'
AND table_name = 'pgbouncer_users'
AND grantee != 'PUBLIC'
GROUP BY grantee;


DROP VIEW @extschema@.pgbouncer_databases;
DROP VIEW @extschema@.pgbouncer_users;

DROP FUNCTION @extschema@.pgbouncer_databases_func();
DROP FUNCTION @extschema@.pgbouncer_users_func();

CREATE FUNCTION @extschema@.pgbouncer_databases_func() RETURNS TABLE
(
    pgbouncer_target_host text
    , name text
    , host text
    , port int
    , database text
    , force_user text
    , pool_size int
    , min_pool_size int
    , reserve_pool_size int
    , server_lifetime int
    , pool_mode text
    , load_balance_hosts int
    , max_connections int
    , current_connections int
    , max_client_connections int 
    , current_client_connections int
    , paused int
    , disabled int
)
LANGUAGE plpgsql
AS $$
DECLARE
    ex_context          text;
    ex_detail           text;
    ex_hint             text;
    ex_message          text;
    v_row               record;
    v_version_major     int;
    v_version_minor     int;
BEGIN

FOR v_row IN
    SELECT target_host FROM @extschema@.pgbouncer_fdw_targets WHERE active
LOOP BEGIN

    SELECT version_major, version_minor
    INTO v_version_major, v_version_minor
    FROM @extschema@.pgbouncer_version_func(v_row.target_host);

    IF v_version_major = 1 THEN
        IF v_version_minor >= 24 THEN
            RETURN QUERY SELECT
                v_row.target_host AS pgbouncer_target_host
                , x.name
                , x.host
                , x.port
                , x.database
                , x.force_user
                , x.pool_size
                , x.min_pool_size
                , x.reserve_pool_size
                , x.server_lifetime
                , x.pool_mode
                , x.load_balance_hosts
                , x.max_connections
                , x.current_connections
                , x.max_client_connections
                , x.current_client_connections
                , x.paused
                , x.disabled
            FROM dblink(v_row.target_host, 'show databases') AS x
            (
                name text
                , host text
                , port int
                , database text
                , force_user text
                , pool_size int
                , min_pool_size int
                , reserve_pool_size int
                , server_lifetime int
                , pool_mode text
                , load_balance_hosts int
                , max_connections int
                , current_connections int
                , max_client_connections int 
                , current_client_connections int
                , paused int
                , disabled int
            );
        ELSIF v_version_minor = 23 THEN
            RETURN QUERY SELECT
                v_row.target_host AS pgbouncer_target_host
                , x.name
                , x.host
                , x.port
                , x.database
                , x.force_user
                , x.pool_size
                , x.min_pool_size
                , x.reserve_pool AS reserve_pool_size
                , x.server_lifetime
                , x.pool_mode
                , 0 AS load_balance_hosts
                , x.max_connections
                , x.current_connections
                , 0 AS max_client_connections 
                , 0 AS current_client_connections
                , x.paused
                , x.disabled
            FROM dblink(v_row.target_host, 'show databases') AS x
            (
                name text
                , host text
                , port int
                , database text
                , force_user text
                , pool_size int
                , min_pool_size int
                , reserve_pool int
                , server_lifetime int
                , pool_mode text
                , max_connections int
                , current_connections int
                , paused int
                , disabled int
            );
        ELSIF v_version_minor < 23 THEN
            RETURN QUERY SELECT
                v_row.target_host AS pgbouncer_target_host
                , x.name
                , x.host
                , x.port
                , x.database
                , x.force_user
                , x.pool_size
                , x.min_pool_size
                , x.reserve_pool AS reserve_pool_size
                , 0 AS server_lifetime
                , x.pool_mode
                , 0 AS load_balance_hosts
                , x.max_connections
                , x.current_connections
                , 0 AS max_client_connections 
                , 0 AS current_client_connections
                , x.paused
                , x.disabled
            FROM dblink(v_row.target_host, 'show databases') AS x
            (
                name text
                , host text
                , port int
                , database text
                , force_user text
                , pool_size int
                , min_pool_size int
                , reserve_pool int
                , pool_mode text
                , max_connections int
                , current_connections int
                , paused int
                , disabled int
            );
        END IF;
    ELSE
        RAISE EXCEPTION 'Encountered unsupported version of PgBouncer: %.%.x', v_version_major, v_version_minor;
    END IF;
    EXCEPTION
        WHEN connection_exception THEN
            RAISE WARNING 'pgbouncer_fdw: Unable to establish connection to pgBouncer target host: %. Continuing to additional hosts.', v_row.target_host;
            GET STACKED DIAGNOSTICS ex_message = MESSAGE_TEXT,
                                    ex_context = PG_EXCEPTION_CONTEXT,
                                    ex_detail = PG_EXCEPTION_DETAIL,
                                    ex_hint = PG_EXCEPTION_HINT;
            RAISE WARNING 'pgbouncer_fdw: Unable to establish connection to pgBouncer target host: %. Continuing to additional hosts.
ORIGINAL ERROR: %
CONTEXT: %
DETAIL: %
HINT: %', v_row.target_host, ex_message, ex_context, ex_detail, ex_hint;
END;
END LOOP;

END
$$;


CREATE VIEW @extschema@.pgbouncer_databases AS
    SELECT pgbouncer_target_host
        , name
        , host
        , port
        , database
        , force_user
        , pool_size
        , min_pool_size
        , reserve_pool_size
        , server_lifetime
        , pool_mode
        , load_balance_hosts
        , max_connections
        , current_connections
        , max_client_connections
        , current_client_connections
        , paused
        , disabled
     FROM @extschema@.pgbouncer_databases_func();


CREATE FUNCTION @extschema@.pgbouncer_users_func() RETURNS TABLE
(
    pgbouncer_target_host text
    , name text
    , pool_size int
    , reserve_pool_size int
    , pool_mode text
    , max_user_connections int
    , current_connections int
    , max_user_client_connections int
    , current_client_connections int
)
LANGUAGE plpgsql
AS $$
DECLARE
    ex_context          text;
    ex_detail           text;
    ex_hint             text;
    ex_message          text;
    v_row               record;
    v_version_major     int;
    v_version_minor     int;
BEGIN

FOR v_row IN
    SELECT target_host FROM @extschema@.pgbouncer_fdw_targets WHERE active
LOOP BEGIN

    SELECT version_major, version_minor
    INTO v_version_major, v_version_minor
    FROM @extschema@.pgbouncer_version_func(v_row.target_host);

    IF v_version_major = 1 THEN
        IF v_version_minor >= 24 THEN
            RETURN QUERY SELECT
                v_row.target_host AS pgbouncer_target_host
                , x.name
                , x.pool_size
                , x.reserve_pool_size
                , x.pool_mode
                , x.max_user_connections
                , x.current_connections
                , x.max_user_client_connections
                , x.current_client_connections
            FROM dblink(v_row.target_host, 'show users') AS x
            (
                name text
                , pool_size int
                , reserve_pool_size int
                , pool_mode text
                , max_user_connections int
                , current_connections int
                , max_user_client_connections int
                , current_client_connections int
            );
        ELSIF v_version_minor = 23 THEN
            RETURN QUERY SELECT
                v_row.target_host AS pgbouncer_target_host
                , x.name
                , x.pool_size
                , 0 AS reserve_pool_size
                , x.pool_mode
                , x.max_user_connections
                , x.current_connections
                , 0 AS max_user_client_connections
                , 0 AS current_client_connections
            FROM dblink(v_row.target_host, 'show users') AS x
            (
                name text
                , pool_size int
                , pool_mode text
                , max_user_connections int
                , current_connections int
            );
        ELSIF v_version_minor < 23 THEN
            RETURN QUERY SELECT
                v_row.target_host AS pgbouncer_target_host
                , x.name
                , 0 AS pool_size
                , 0 AS reserve_pool_size
                , x.pool_mode
                , 0 AS max_user_connections
                , 0 AS current_connections
                , 0 AS max_user_client_connections
                , 0 AS current_client_connections
            FROM dblink(v_row.target_host, 'show users') AS x
            (
                name text
                , pool_mode text
            );
        END IF;
    ELSE
        RAISE EXCEPTION 'Encountered unsupported version of PgBouncer: %.%.x', v_version_major, v_version_minor;
    END IF;
    EXCEPTION
        WHEN connection_exception THEN
            RAISE WARNING 'pgbouncer_fdw: Unable to establish connection to pgBouncer target host: %. Continuing to additional hosts.', v_row.target_host;
            GET STACKED DIAGNOSTICS ex_message = MESSAGE_TEXT,
                                    ex_context = PG_EXCEPTION_CONTEXT,
                                    ex_detail = PG_EXCEPTION_DETAIL,
                                    ex_hint = PG_EXCEPTION_HINT;
            RAISE WARNING 'pgbouncer_fdw: Unable to establish connection to pgBouncer target host: %. Continuing to additional hosts.
ORIGINAL ERROR: %
CONTEXT: %
DETAIL: %
HINT: %', v_row.target_host, ex_message, ex_context, ex_detail, ex_hint;
END;
END LOOP;
END
$$;


CREATE VIEW @extschema@.pgbouncer_users AS
    SELECT pgbouncer_target_host
        , name
        , pool_size
        , reserve_pool_size
        , pool_mode
        , max_user_connections
        , current_connections
        , max_user_client_connections
        , current_client_connections
     FROM @extschema@.pgbouncer_users_func();

-- Restore dropped object privileges
DO $$
DECLARE
v_row   record;
BEGIN
    FOR v_row IN SELECT statement FROM pgbouncer_fdw_preserve_privs_temp LOOP
        IF v_row.statement IS NOT NULL THEN
            EXECUTE v_row.statement;
        END IF;
    END LOOP;
END
$$;

DROP TABLE IF EXISTS pgbouncer_fdw_preserve_privs_temp;
