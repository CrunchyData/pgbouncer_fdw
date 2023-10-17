CREATE TEMP TABLE pgbouncer_fdw_preserve_privs_temp (statement text);


INSERT INTO pgbouncer_fdw_preserve_privs_temp 
SELECT 'GRANT EXECUTE ON FUNCTION @extschema@.pgbouncer_clients_func()  TO '||array_to_string(array_agg('"'||grantee::text||'"'), ',')||';' 
FROM information_schema.routine_privileges
WHERE routine_schema = '@extschema@'
AND routine_name = 'pgbouncer_clients_func';

INSERT INTO pgbouncer_fdw_preserve_privs_temp 
SELECT 'GRANT EXECUTE ON FUNCTION @extschema@.pgbouncer_servers_func()  TO '||array_to_string(array_agg('"'||grantee::text||'"'), ',')||';' 
FROM information_schema.routine_privileges
WHERE routine_schema = '@extschema@'
AND routine_name = 'pgbouncer_servers_func';

INSERT INTO pgbouncer_fdw_preserve_privs_temp 
SELECT 'GRANT EXECUTE ON FUNCTION @extschema@.pgbouncer_sockets_func()  TO '||array_to_string(array_agg('"'||grantee::text||'"'), ',')||';' 
FROM information_schema.routine_privileges
WHERE routine_schema = '@extschema@'
AND routine_name = 'pgbouncer_sockets_func';

DROP FUNCTION @extschema@.pgbouncer_clients_func();
DROP FUNCTION @extschema@.pgbouncer_servers_func();
DROP FUNCTION @extschema@.pgbouncer_sockets_func();


INSERT INTO pgbouncer_fdw_preserve_privs_temp
SELECT 'GRANT '||string_agg(privilege_type, ',')||' ON @extschema@.pgbouncer_clients TO '||grantee::text||';'
FROM information_schema.table_privileges
WHERE table_schema = '@extschema@'
AND table_name = 'pgbouncer_clients'
GROUP BY grantee;

INSERT INTO pgbouncer_fdw_preserve_privs_temp
SELECT 'GRANT '||string_agg(privilege_type, ',')||' ON @extschema@.pgbouncer_servers TO '||grantee::text||';'
FROM information_schema.table_privileges
WHERE table_schema = '@extschema@'
AND table_name = 'pgbouncer_servers'
GROUP BY grantee;

INSERT INTO pgbouncer_fdw_preserve_privs_temp
SELECT 'GRANT '||string_agg(privilege_type, ',')||' ON @extschema@.pgbouncer_sockets TO '||grantee::text||';'
FROM information_schema.table_privileges
WHERE table_schema = '@extschema@'
AND table_name = 'pgbouncer_sockets'
GROUP BY grantee;

DROP VIEW @extschema@.pgbouncer_clients;
DROP VIEW @extschema@.pgbouncer_servers;
DROP VIEW @extschema@.pgbouncer_sockets;


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
    ELSIF v_version_major >= 1 AND v_version_minor >= 18 AND v_version_minor < 21 THEN 
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
    IF v_version_major >= 1 AND v_version_minor >= 18 and v_version_minor < 21 THEN 
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
    IF v_version_major >= 1 AND v_version_minor >= 18 AND v_version_minor < 21 THEN 
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
        , prepared_statements
    FROM @extschema@.pgbouncer_clients_func();


CREATE VIEW @extschema@.pgbouncer_servers AS
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
        , prepared_statements
     FROM @extschema@.pgbouncer_servers_func();


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
        , prepared_statements
     FROM @extschema@.pgbouncer_sockets_func();

