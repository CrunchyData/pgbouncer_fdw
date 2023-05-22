-- IMPORTANT NOTE: All objects in this extension are dropped and recreated as part of this update. Privileges ARE NOT preserved as part of this update, so please ensure privileges you have on these objects are preserved before upgrading so that they can be reapplied. Note that execution by PUBLIC on the admin functions is once again revoked by this update.

-- Add support for gathering statistics from multiple pgBouncer targets
  -- A new configuration table has been added to define the names of all FDW servers.
  -- All administrative command functions have had a parameter for the FDW server name added to them. These functions intentionally do not use the configuration table to avoid accidentally running an admin command on multiple servers.

-- Add better support for multiple versions of pgBouncer. Support for 1.17 has been backported into this version of pgbouncer_fdw.

DROP VIEW @extschema@.pgbouncer_clients;
DROP VIEW @extschema@.pgbouncer_config;
DROP VIEW @extschema@.pgbouncer_databases;
DROP VIEW @extschema@.pgbouncer_dns_hosts;
DROP VIEW @extschema@.pgbouncer_dns_zones;
DROP VIEW @extschema@.pgbouncer_lists;
DROP VIEW @extschema@.pgbouncer_pools;
DROP VIEW @extschema@.pgbouncer_servers;
DROP VIEW @extschema@.pgbouncer_sockets;
DROP VIEW @extschema@.pgbouncer_stats;
DROP VIEW @extschema@.pgbouncer_users;

DROP FUNCTION @extschema@.pgbouncer_command_disable(text);
DROP FUNCTION @extschema@.pgbouncer_command_enable(text);
DROP FUNCTION @extschema@.pgbouncer_command_kill(text);
DROP FUNCTION @extschema@.pgbouncer_command_pause(text);
DROP FUNCTION @extschema@.pgbouncer_command_reconnect(text);
DROP FUNCTION @extschema@.pgbouncer_command_reload();
DROP FUNCTION @extschema@.pgbouncer_command_resume(text);
DROP FUNCTION @extschema@.pgbouncer_command_set(text, text);
DROP FUNCTION @extschema@.pgbouncer_command_shutdown();
DROP FUNCTION @extschema@.pgbouncer_command_suspend();
DROP FUNCTION @extschema@.pgbouncer_command_wait_close(text);

/*
 * pgbouncer_fdw_targets 
 */
CREATE TABLE @extschema@.pgbouncer_fdw_targets (
    target_host text NOT NULL
    , active boolean NOT NULL DEFAULT true
    , CONSTRAINT pgbouncer_fdw_targets_pk PRIMARY KEY (target_host) );
CREATE INDEX pgbouncer_fdw_targets_active_idx ON pgbouncer_fdw_targets (active);
SELECT pg_catalog.pg_extension_config_dump('pgbouncer_fdw_targets', '');

INSERT INTO @extschema@.pgbouncer_fdw_targets ( target_host ) VALUES ('pgbouncer');


/*
 * pgbouncer_version_func
 */
CREATE FUNCTION  @extschema@.pgbouncer_version_func(p_target_host text DEFAULT NULL) RETURNS TABLE
(
    pgbouncer_target_host text
    , version_major int
    , version_minor int
    , version_patch int
)
LANGUAGE plpgsql
AS $$
DECLARE
    ex_context                      text;
    ex_detail                       text;
    ex_hint                         text;
    ex_message                      text;
    v_row   record;
    v_sql   text;
BEGIN

v_sql := 'SELECT target_host FROM @extschema@.pgbouncer_fdw_targets WHERE active';
IF p_target_host IS NOT NULL THEN
    v_sql := v_sql || format(' AND target_host = %L', p_target_host);
END IF;

FOR v_row IN EXECUTE v_sql
LOOP BEGIN
    RETURN QUERY SELECT 
        v_row.target_host AS pgbouncer_target_host
        , split_part(substring(version from '\d.+'), '.', 1)::int AS version_major
        , split_part(substring(version from '\d.+'), '.', 2)::int AS version_minor
        , split_part(substring(version from '\d.+'), '.', 3)::int AS version_patch
    FROM dblink(v_row.target_host, 'show version') AS x
    (   
        version text
    );
    EXCEPTION
        WHEN connection_exception THEN
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

CREATE VIEW @extschema@.pgbouncer_version AS
    SELECT pgbouncer_target_host
        , version_major
        , version_minor
        , version_patch
    FROM @extschema@.pgbouncer_version_func();


/*
 * pgbouncer_clients_func
 */
CREATE FUNCTION @extschema@.pgbouncer_clients_func() RETURNS TABLE 
( 
    pgbouncer_target_host text
    , "type" text
    , "user" text
    , database text
    , state text
    , addr text
    , port int
    , local_addr text
    , local_port int
    , connect_time timestamp with time zone
    , request_time timestamp with time zone
    , wait int
    , wait_us int
    , close_needed int
    , ptr text
    , link text
    , remote_pid int
    , tls text
    , application_name text
)
LANGUAGE plpgsql
AS $$
DECLARE
    ex_context                      text;
    ex_detail                       text;
    ex_hint                         text;
    ex_message                      text;
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
  
    IF v_version_major >= 1 AND v_version_minor >= 18 THEN 
        RETURN QUERY SELECT 
           v_row.target_host AS pgbouncer_target_host
           , x."type"
           , x."user"
           , x.database
           , x.state
           , x.addr
           , x.port
           , x.local_addr
           , x.local_port
           , x.connect_time
           , x.request_time
           , x.wait
           , x.wait_us
           , x.close_needed
           , x.ptr
           , x.link
           , x.remote_pid
           , x.tls
           , x.application_name
        FROM dblink(v_row.target_host, 'show clients') AS x
        (  "type" text
           , "user" text
           , database text
           , state text
           , addr text
           , port int
           , local_addr text
           , local_port int
           , connect_time timestamp with time zone
           , request_time timestamp with time zone
           , wait int
           , wait_us int
           , close_needed int
           , ptr text
           , link text
           , remote_pid int
           , tls text
           , application_name text
        );
    -- backward compatiblity floor is 1.17
    ELSIF v_version_major = 1 AND v_version_minor = 17 THEN 
        RETURN QUERY SELECT 
           v_row.target_host AS pgbouncer_target_host
           , x."type"
           , x."user"
           , x.database
           , x.state
           , x.addr
           , x.port
           , x.local_addr
           , x.local_port
           , x.connect_time
           , x.request_time
           , x.wait
           , x.wait_us
           , x.close_needed
           , x.ptr
           , x.link
           , x.remote_pid
           , x.tls
           , '' AS application_name
        FROM dblink(v_row.target_host, 'show clients') AS x
        (  "type" text
           , "user" text
           , database text
           , state text
           , addr text
           , port int
           , local_addr text
           , local_port int
           , connect_time timestamp with time zone
           , request_time timestamp with time zone
           , wait int
           , wait_us int
           , close_needed int
           , ptr text
           , link text
           , remote_pid int
           , tls text
        );
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

CREATE VIEW @extschema@.pgbouncer_clients AS
    SELECT pgbouncer_target_host
        , "type"
        , "user"
        , database
        , state
        , addr
        , port
        , local_addr
        , local_port
        , connect_time
        , request_time
        , wait
        , wait_us
        , close_needed
        , ptr
        , link
        , remote_pid
        , tls
        , application_name
    FROM @extschema@.pgbouncer_clients_func();


/*
 * pgbouncer_config_func
 */
CREATE FUNCTION @extschema@.pgbouncer_config_func() RETURNS TABLE 
( 
    pgbouncer_target_host text
    , key text
    , value text
    , "default" text
    , changeable boolean
)
LANGUAGE plpgsql
AS $$
DECLARE
    ex_context                      text;
    ex_detail                       text;
    ex_hint                         text;
    ex_message                      text;
    v_row       record;
BEGIN

FOR v_row IN  
    SELECT target_host FROM @extschema@.pgbouncer_fdw_targets WHERE active
LOOP BEGIN

    RETURN QUERY SELECT 
        v_row.target_host AS pgbouncer_target_host
        , x.key
        , x.value
        , x."default"
        , x.changeable
    FROM dblink(v_row.target_host, 'show config') AS x
    (   key text
        , value text
        , "default" text
        , changeable boolean
    );
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

CREATE VIEW @extschema@.pgbouncer_config AS
    SELECT pgbouncer_target_host
        , key
        , value
        , "default"
        , changeable
    FROM @extschema@.pgbouncer_config_func();


/*
 * pgbouncer_databases_func
 */
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
    , reserve_pool int
    , pool_mode text
    , max_connections int
    , current_connections int
    , paused int
    , disabled int
)
LANGUAGE plpgsql
AS $$
DECLARE
    ex_context                      text;
    ex_detail                       text;
    ex_hint                         text;
    ex_message                      text;
    v_row       record;
BEGIN

FOR v_row IN  
    SELECT target_host FROM @extschema@.pgbouncer_fdw_targets WHERE active
LOOP BEGIN

    RETURN QUERY SELECT 
        v_row.target_host AS pgbouncer_target_host
        , x.name
        , x.host
        , x.port
        , x.database
        , x.force_user
        , x.pool_size
        , x.min_pool_size
        , x.reserve_pool
        , x.pool_mode
        , x.max_connections
        , x.current_connections
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
        , reserve_pool
        , pool_mode
        , max_connections
        , current_connections
        , paused
        , disabled
     FROM @extschema@.pgbouncer_databases_func();


/*
 * pgbouncer_dns_hosts_func
 */
CREATE FUNCTION @extschema@.pgbouncer_dns_hosts_func() RETURNS TABLE 
( 
    pgbouncer_target_host text
    , hostname text
    , ttl bigint
    , addrs text
)
LANGUAGE plpgsql
AS $$
DECLARE
    ex_context                      text;
    ex_detail                       text;
    ex_hint                         text;
    ex_message                      text;
    v_row       record;
BEGIN

FOR v_row IN  
    SELECT target_host FROM @extschema@.pgbouncer_fdw_targets WHERE active
LOOP BEGIN

    RETURN QUERY SELECT 
        v_row.target_host AS pgbouncer_target_host
        , x.hostname
        , x.ttl
        , x.addrs
    FROM dblink(v_row.target_host, 'show dns_hosts') AS x
    (   
        hostname text
        , ttl bigint
        , addrs text
    );
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

CREATE VIEW @extschema@.pgbouncer_dns_hosts AS
    SELECT pgbouncer_target_host
        , hostname
        , ttl
        , addrs
     FROM @extschema@.pgbouncer_dns_hosts_func();

/*
 * pgbouncer_dns_zones_func
 */
CREATE FUNCTION @extschema@.pgbouncer_dns_zones_func() RETURNS TABLE 
( 
    pgbouncer_target_host text
    , zonename text
    , serial text
    , count int
)
LANGUAGE plpgsql
AS $$
DECLARE
    ex_context                      text;
    ex_detail                       text;
    ex_hint                         text;
    ex_message                      text;
    v_row       record;
BEGIN

FOR v_row IN  
    SELECT target_host FROM @extschema@.pgbouncer_fdw_targets WHERE active
LOOP BEGIN

    RETURN QUERY SELECT 
        v_row.target_host AS pgbouncer_target_host
        , x.zonename
        , x.serial
        , x.count
    FROM dblink(v_row.target_host, 'show dns_zones') AS x
    (   
        zonename text
        , serial text
        , count int
    );
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

CREATE VIEW @extschema@.pgbouncer_dns_zones AS
    SELECT pgbouncer_target_host
        zonename
        , serial
        , count
     FROM @extschema@.pgbouncer_dns_zones_func();


/*
 * pgbouncer_lists_func
 */ 
CREATE FUNCTION @extschema@.pgbouncer_lists_func() RETURNS TABLE 
( 
    pgbouncer_target_host text
    , list text
    , items int
)
LANGUAGE plpgsql
AS $$
DECLARE
    ex_context                      text;
    ex_detail                       text;
    ex_hint                         text;
    ex_message                      text;
    v_row       record;
BEGIN

FOR v_row IN  
    SELECT target_host FROM @extschema@.pgbouncer_fdw_targets WHERE active
LOOP BEGIN

    RETURN QUERY SELECT 
        v_row.target_host AS pgbouncer_target_host
        , x.list
        , x.items
    FROM dblink(v_row.target_host, 'show lists') AS x
    (   
        list text
        , items int
    );
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

CREATE VIEW @extschema@.pgbouncer_lists AS
    SELECT pgbouncer_target_host
        , list
        , items
     FROM @extschema@.pgbouncer_lists_func();


/*
 * pgbouncer_pools_func
 */
CREATE FUNCTION @extschema@.pgbouncer_pools_func() RETURNS TABLE 
( 
    pgbouncer_target_host text
    , database text
    , "user" text
    , cl_active int
    , cl_waiting int
    , cl_active_cancel_req int
    , cl_waiting_cancel_req int
    , sv_active int
    , sv_active_cancel int
    , sv_being_canceled int
    , sv_idle int
    , sv_used int
    , sv_tested int
    , sv_login int
    , maxwait int
    , maxwait_us int
    , pool_mode text
)
LANGUAGE plpgsql
AS $$
DECLARE
    ex_context                      text;
    ex_detail                       text;
    ex_hint                         text;
    ex_message                      text;
    v_row       record;
    v_version_major     int;
    v_version_minor     int;
BEGIN

FOR v_row IN  
    SELECT target_host FROM @extschema@.pgbouncer_fdw_targets WHERE active
LOOP BEGIN

    SELECT version_major, version_minor
    INTO v_version_major, v_version_minor
    FROM @extschema@.pgbouncer_version_func(v_row.target_host);
  
    IF v_version_major >= 1 AND v_version_minor >= 18 THEN 
        RETURN QUERY SELECT 
               v_row.target_host AS pgbouncer_target_host
               , x.database
               , x."user"
               , x.cl_active
               , x.cl_waiting
               , x.cl_active_cancel_req
               , x.cl_waiting_cancel_req
               , x.sv_active
               , x.sv_active_cancel
               , x.sv_being_canceled
               , x.sv_idle
               , x.sv_used
               , x.sv_tested
               , x.sv_login
               , x.maxwait
               , x.maxwait_us
               , x.pool_mode
        FROM dblink(v_row.target_host, 'show pools') AS x
        (   database text
            , "user" text
            , cl_active int
            , cl_waiting int
            , cl_active_cancel_req int
            , cl_waiting_cancel_req int
            , sv_active int
            , sv_active_cancel int
            , sv_being_canceled int
            , sv_idle int
            , sv_used int
            , sv_tested int
            , sv_login int
            , maxwait int
            , maxwait_us int
            , pool_mode text
        );
    -- backward compatiblity floor is 1.17
    -- old cl_cancel_req is sent as cl_waiting_cancel_req
    ELSIF v_version_major = 1 AND v_version_minor = 17 THEN 
        RETURN QUERY SELECT 
               v_row.target_host AS pgbouncer_target_host
               , x.database
               , x."user"
               , x.cl_active
               , x.cl_waiting
               , 0 AS cl_active_cancel_req
               , x.cl_cancel_req AS cl_waiting_cancel_req
               , x.sv_active
               , 0 AS sv_active_cancel
               , 0 AS sv_being_canceled
               , x.sv_idle
               , x.sv_used
               , x.sv_tested
               , x.sv_login
               , x.maxwait
               , x.maxwait_us
               , x.pool_mode
        FROM dblink(v_row.target_host, 'show pools') AS x
        (   database text
            , "user" text
            , cl_active int
            , cl_waiting int
            , cl_cancel_req int
            , sv_active int
            , sv_idle int
            , sv_used int
            , sv_tested int
            , sv_login int
            , maxwait int
            , maxwait_us int
            , pool_mode text
        );
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

CREATE VIEW @extschema@.pgbouncer_pools AS
    SELECT pgbouncer_target_host 
        , database
        , "user"
        , cl_active
        , cl_waiting
        , cl_active_cancel_req
        , cl_waiting_cancel_req
        , sv_active
        , sv_active_cancel
        , sv_being_canceled
        , sv_idle
        , sv_used
        , sv_tested
        , sv_login
        , maxwait
        , maxwait_us
        , pool_mode
    FROM @extschema@.pgbouncer_pools_func();


/*
 * pgbouncer_servers_func
 */
CREATE FUNCTION @extschema@.pgbouncer_servers_func() RETURNS TABLE 
( 
    pgbouncer_target_host text
    , "type" text
    , "user" text
    , database text
    , state text
    , addr text
    , port int
    , local_addr text
    , local_port int
    , connect_time timestamp with time zone
    , request_time timestamp with time zone
    , wait int
    , wait_us int
    , close_needed int
    , ptr text
    , link text
    , remote_pid int
    , tls text
    , application_name text
)
LANGUAGE plpgsql
AS $$
DECLARE
    ex_context                      text;
    ex_detail                       text;
    ex_hint                         text;
    ex_message                      text;
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
  
    IF v_version_major >= 1 AND v_version_minor >= 18 THEN 
        RETURN QUERY SELECT 
            v_row.target_host AS pgbouncer_target_host
            , x."type"
            , x."user"
            , x.database
            , x.state
            , x.addr
            , x.port
            , x.local_addr
            , x.local_port
            , x.connect_time
            , x.request_time
            , x.wait
            , x.wait_us
            , x.close_needed
            , x.ptr
            , x.link
            , x.remote_pid
            , x.tls
            , x.application_name
        FROM dblink(v_row.target_host, 'show servers') AS x
        (   
            "type" text
            , "user" text
            , database text
            , state text
            , addr text
            , port int
            , local_addr text
            , local_port int
            , connect_time timestamp with time zone
            , request_time timestamp with time zone
            , wait int
            , wait_us int
            , close_needed int
            , ptr text
            , link text
            , remote_pid int
            , tls text
            , application_name text
        );
    -- backward compatiblity floor is 1.17
    ELSIF v_version_major = 1 AND v_version_minor = 17 THEN 
        RETURN QUERY SELECT 
            v_row.target_host AS pgbouncer_target_host
            , x."type"
            , x."user"
            , x.database
            , x.state
            , x.addr
            , x.port
            , x.local_addr
            , x.local_port
            , x.connect_time
            , x.request_time
            , x.wait
            , x.wait_us
            , x.close_needed
            , x.ptr
            , x.link
            , x.remote_pid
            , x.tls
            , '' AS application_name
        FROM dblink(v_row.target_host, 'show servers') AS x
        (   
            "type" text
            , "user" text
            , database text
            , state text
            , addr text
            , port int
            , local_addr text
            , local_port int
            , connect_time timestamp with time zone
            , request_time timestamp with time zone
            , wait int
            , wait_us int
            , close_needed int
            , ptr text
            , link text
            , remote_pid int
            , tls text
        );
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

CREATE VIEW @extschema@.pgbouncer_servers AS
    SELECT pgbouncer_target_host
        "type"
        , "user"
        , database
        , state
        , addr
        , port
        , local_addr
        , local_port
        , connect_time
        , request_time
        , wait
        , wait_us
        , close_needed
        , ptr
        , link
        , remote_pid
        , tls
        , application_name
     FROM @extschema@.pgbouncer_servers_func();


/*
 * pgbouncer_sockets_func
 */
CREATE FUNCTION @extschema@.pgbouncer_sockets_func() RETURNS TABLE 
( 
    pgbouncer_target_host text
    , "type" text
    , "user" text
    , database text
    , state text
    , addr text
    , port int
    , local_addr text
    , local_port int
    , connect_time timestamp with time zone
    , request_time timestamp with time zone
    , wait int
    , wait_us int
    , close_needed int
    , ptr text
    , link text
    , remote_pid int
    , tls text
    , application_name text
    , recv_pos int
    , pkt_pos int
    , pkt_remain int
    , send_pos int
    , send_remain int
    , pkt_avail int
    , send_avail int
)
LANGUAGE plpgsql
AS $$
DECLARE
    ex_context                      text;
    ex_detail                       text;
    ex_hint                         text;
    ex_message                      text;
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
  
    IF v_version_major >= 1 AND v_version_minor >= 18 THEN 
        RETURN QUERY SELECT 
            v_row.target_host AS pgbouncer_target_host
            , x."type"
            , x."user"
            , x.database
            , x.state
            , x.addr
            , x.port
            , x.local_addr
            , x.local_port
            , x.connect_time
            , x.request_time
            , x.wait
            , x.wait_us
            , x.close_needed
            , x.ptr
            , x.link
            , x.remote_pid
            , x.tls
            , x.application_name
            , x.recv_pos
            , x.pkt_pos
            , x.pkt_remain
            , x.send_pos
            , x.send_remain
            , x.pkt_avail
            , x.send_avail
        FROM dblink(v_row.target_host, 'show sockets') AS x
        (   
            "type" text
            , "user" text
            , database text
            , state text
            , addr text
            , port int
            , local_addr text
            , local_port int
            , connect_time timestamp with time zone
            , request_time timestamp with time zone
            , wait int
            , wait_us int
            , close_needed int
            , ptr text
            , link text
            , remote_pid int
            , tls text
            , application_name text
            , recv_pos int
            , pkt_pos int
            , pkt_remain int
            , send_pos int
            , send_remain int
            , pkt_avail int
            , send_avail int
        );
    -- backward compatiblity floor is 1.17
    ELSIF v_version_major = 1 AND v_version_minor = 17 THEN 
        RETURN QUERY SELECT 
            v_row.target_host AS pgbouncer_target_host
            , x."type"
            , x."user"
            , x.database
            , x.state
            , x.addr
            , x.port
            , x.local_addr
            , x.local_port
            , x.connect_time
            , x.request_time
            , x.wait
            , x.wait_us
            , x.close_needed
            , x.ptr
            , x.link
            , x.remote_pid
            , x.tls
            , '' AS application_name
            , x.recv_pos
            , x.pkt_pos
            , x.pkt_remain
            , x.send_pos
            , x.send_remain
            , x.pkt_avail
            , x.send_avail
        FROM dblink(v_row.target_host, 'show sockets') AS x
        (   
            "type" text
            , "user" text
            , database text
            , state text
            , addr text
            , port int
            , local_addr text
            , local_port int
            , connect_time timestamp with time zone
            , request_time timestamp with time zone
            , wait int
            , wait_us int
            , close_needed int
            , ptr text
            , link text
            , remote_pid int
            , tls text
            , recv_pos int
            , pkt_pos int
            , pkt_remain int
            , send_pos int
            , send_remain int
            , pkt_avail int
            , send_avail int
        );
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

CREATE VIEW @extschema@.pgbouncer_sockets AS
    SELECT pgbouncer_target_host
        , "type"
        , "user"
        , database
        , state
        , addr
        , port
        , local_addr
        , local_port
        , connect_time
        , request_time
        , wait
        , wait_us
        , close_needed
        , ptr
        , link
        , remote_pid
        , tls
        , application_name
        , recv_pos
        , pkt_pos
        , pkt_remain
        , send_pos
        , send_remain
        , pkt_avail
        , send_avail
     FROM @extschema@.pgbouncer_sockets_func();


/*
 * pgbouncer_stats_func
 */
CREATE FUNCTION @extschema@.pgbouncer_stats_func() RETURNS TABLE 
( 
    pgbouncer_target_host text
    , database text
    , total_xact_count bigint
    , total_query_count bigint
    , total_received bigint
    , total_sent bigint
    , total_xact_time bigint
    , total_query_time bigint
    , total_wait_time bigint
    , avg_xact_count bigint
    , avg_query_count bigint
    , avg_recv bigint
    , avg_sent bigint
    , avg_xact_time bigint
    , avg_query_time bigint
    , avg_wait_time bigint
)
LANGUAGE plpgsql
AS $$
DECLARE
    ex_context                      text;
    ex_detail                       text;
    ex_hint                         text;
    ex_message                      text;
    v_row       record;
BEGIN

FOR v_row IN  
    SELECT target_host FROM @extschema@.pgbouncer_fdw_targets WHERE active
LOOP BEGIN

    RETURN QUERY SELECT 
        v_row.target_host AS pgbouncer_target_host
        , x.database
        , x.total_xact_count
        , x.total_query_count
        , x.total_received
        , x.total_sent
        , x.total_xact_time
        , x.total_query_time
        , x.total_wait_time
        , x.avg_xact_count
        , x.avg_query_count
        , x.avg_recv
        , x.avg_sent
        , x.avg_xact_time
        , x.avg_query_time
        , x.avg_wait_time
    FROM dblink(v_row.target_host, 'show stats') AS x
    (   
        database text
        , total_xact_count bigint
        , total_query_count bigint
        , total_received bigint
        , total_sent bigint
        , total_xact_time bigint
        , total_query_time bigint
        , total_wait_time bigint
        , avg_xact_count bigint
        , avg_query_count bigint
        , avg_recv bigint
        , avg_sent bigint
        , avg_xact_time bigint
        , avg_query_time bigint
        , avg_wait_time bigint
    );
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

CREATE VIEW @extschema@.pgbouncer_stats AS
    SELECT pgbouncer_target_host
        , database
        , total_xact_count
        , total_query_count
        , total_received
        , total_sent
        , total_xact_time
        , total_query_time
        , total_wait_time
        , avg_xact_count
        , avg_query_count
        , avg_recv
        , avg_sent
        , avg_xact_time
        , avg_query_time
        , avg_wait_time
     FROM @extschema@.pgbouncer_stats_func();

/*
 * pgbouncer_users_func
 */
CREATE FUNCTION @extschema@.pgbouncer_users_func() RETURNS TABLE 
( 
    pgbouncer_target_host text
    , name text
    , pool_mode text
)
LANGUAGE plpgsql
AS $$
DECLARE
    ex_context                      text;
    ex_detail                       text;
    ex_hint                         text;
    ex_message                      text;
    v_row       record;
BEGIN

FOR v_row IN  
    SELECT target_host FROM @extschema@.pgbouncer_fdw_targets WHERE active
LOOP BEGIN

    RETURN QUERY SELECT 
        v_row.target_host AS pgbouncer_target_host
        , x.name
        , x.pool_mode
    FROM dblink(v_row.target_host, 'show users') AS x
    (   
        name text
        , pool_mode text
    );
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
        , pool_mode
     FROM @extschema@.pgbouncer_users_func();


/**** ADMIN FUNCTIONS */

CREATE FUNCTION @extschema@.pgbouncer_command_disable(p_dbname text, p_pgbouncer_target_host text DEFAULT 'pgbouncer') RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM dblink_exec(p_pgbouncer_target_host, format('DISABLE %I', p_dbname));
END
$$;

CREATE FUNCTION @extschema@.pgbouncer_command_enable(p_dbname text, p_pgbouncer_target_host text DEFAULT 'pgbouncer') RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM dblink_exec(p_pgbouncer_target_host, format('ENABLE %I', p_dbname));
END
$$;

CREATE FUNCTION @extschema@.pgbouncer_command_kill(p_dbname text DEFAULT NULL, p_pgbouncer_target_host text DEFAULT 'pgbouncer') RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    IF p_dbname IS NULL THEN
        PERFORM dblink_exec(p_pgbouncer_target_host, 'KILL');
    ELSE
        PERFORM dblink_exec(p_pgbouncer_target_host, format('KILL %I', p_dbname));
    END IF;
END
$$;

CREATE FUNCTION @extschema@.pgbouncer_command_pause(p_dbname text DEFAULT NULL, p_pgbouncer_target_host text DEFAULT 'pgbouncer') RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    IF p_dbname IS NULL THEN
        PERFORM dblink_exec(p_pgbouncer_target_host, 'PAUSE');
    ELSE
        PERFORM dblink_exec(p_pgbouncer_target_host, format('PAUSE %I', p_dbname));
    END IF;
END
$$;

CREATE FUNCTION @extschema@.pgbouncer_command_reconnect(p_dbname text DEFAULT NULL, p_pgbouncer_target_host text DEFAULT 'pgbouncer') RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    IF p_dbname IS NULL THEN
        PERFORM dblink_exec(p_pgbouncer_target_host, 'RECONNECT');
    ELSE
        PERFORM dblink_exec(p_pgbouncer_target_host, format('RECONNECT %I', p_dbname));
    END IF;
END
$$;

CREATE FUNCTION @extschema@.pgbouncer_command_reload(p_pgbouncer_target_host text DEFAULT 'pgbouncer') RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM dblink_exec(p_pgbouncer_target_host, 'RELOAD');
END
$$;

CREATE FUNCTION @extschema@.pgbouncer_command_resume(p_dbname text DEFAULT NULL, p_pgbouncer_target_host text DEFAULT 'pgbouncer') RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    IF p_dbname IS NULL THEN
        PERFORM dblink_exec(p_pgbouncer_target_host, 'RESUME');
    ELSE
        PERFORM dblink_exec(p_pgbouncer_target_host, format('RESUME %I', p_dbname));
    END IF;
END
$$;

CREATE FUNCTION @extschema@.pgbouncer_command_set(p_name text, p_value text, p_pgbouncer_target_host text DEFAULT 'pgbouncer') RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM dblink_exec(p_pgbouncer_target_host, format('SET %s = %L', p_name, p_value));
END
$$;

CREATE FUNCTION @extschema@.pgbouncer_command_shutdown(p_pgbouncer_target_host text DEFAULT 'pgbouncer') RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM dblink_exec(p_pgbouncer_target_host, 'shutdown');
END
$$;

CREATE FUNCTION @extschema@.pgbouncer_command_suspend(p_pgbouncer_target_host text DEFAULT 'pgbouncer') RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM dblink_exec(p_pgbouncer_target_host, 'SUSPEND');
END
$$;

CREATE FUNCTION @extschema@.pgbouncer_command_wait_close(p_dbname text DEFAULT NULL, p_pgbouncer_target_host text DEFAULT 'pgbouncer') RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    IF p_dbname IS NULL THEN
        PERFORM dblink_exec(p_pgbouncer_target_host, 'WAIT_CLOSE');
    ELSE
        PERFORM dblink_exec(p_pgbouncer_target_host, format('WAIT_CLOSE %I', p_dbname));
    END IF;
END
$$;

REVOKE EXECUTE ON FUNCTION @extschema@.pgbouncer_command_disable(text, text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION @extschema@.pgbouncer_command_enable(text, text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION @extschema@.pgbouncer_command_kill(text, text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION @extschema@.pgbouncer_command_pause(text, text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION @extschema@.pgbouncer_command_reconnect(text, text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION @extschema@.pgbouncer_command_reload(text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION @extschema@.pgbouncer_command_resume(text, text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION @extschema@.pgbouncer_command_set(text, text, text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION @extschema@.pgbouncer_command_shutdown(text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION @extschema@.pgbouncer_command_suspend(text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION @extschema@.pgbouncer_command_wait_close(text, text) FROM PUBLIC;

