
/**** VIEW FUNCTIONS ****/

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
    , prepared_statements int
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
  
    IF v_version_major >= 1 AND v_version_minor >= 21 THEN 
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
           , x.prepared_statements
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
           , prepared_statements int
        );
    ELSIF v_version_major = 1 AND v_version_minor >= 18 AND v_version_minor < 21 THEN 
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
           , 0 AS prepared_statements
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
           , 0 AS prepared_statements
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
    , prepared_statements int
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
  
    IF v_version_major >= 1 AND v_version_minor >= 21 THEN 
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
            , x.prepared_statements
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
            , prepared_statements int
        );
    ELSIF v_version_major = 1 AND v_version_minor >= 18 and v_version_minor < 21 THEN 
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
            , 0 AS prepared_statements
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
            , 0 AS prepared_statements
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
    , prepared_statements int
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
  
    IF v_version_major >= 1 AND v_version_minor >= 21 THEN 
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
            , x.prepared_statements
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
            , prepared_statements int
        );
    ELSIF v_version_major = 1 AND v_version_minor >= 18 AND v_version_minor < 21 THEN 
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
            , 0 AS prepared_statements
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
            , 0 AS prepared_statements
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


/**** ADMIN FUNCTIONS ****/

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

