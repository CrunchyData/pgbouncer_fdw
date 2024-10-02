CREATE TEMP TABLE pgbouncer_fdw_preserve_privs_temp (statement text);

INSERT INTO pgbouncer_fdw_preserve_privs_temp
SELECT 'GRANT '||string_agg(privilege_type, ',')||' ON @extschema@.pgbouncer_clients TO '||grantee::text||';'
FROM information_schema.table_privileges
WHERE table_schema = '@extschema@'
AND table_name = 'pgbouncer_clients'
AND grantee != 'PUBLIC'
GROUP BY grantee;

INSERT INTO pgbouncer_fdw_preserve_privs_temp
SELECT 'GRANT '||string_agg(privilege_type, ',')||' ON @extschema@.pgbouncer_servers TO '||grantee::text||';'
FROM information_schema.table_privileges
WHERE table_schema = '@extschema@'
AND table_name = 'pgbouncer_servers'
AND grantee != 'PUBLIC'
GROUP BY grantee;

INSERT INTO pgbouncer_fdw_preserve_privs_temp
SELECT 'GRANT '||string_agg(privilege_type, ',')||' ON @extschema@.pgbouncer_sockets TO '||grantee::text||';'
FROM information_schema.table_privileges
WHERE table_schema = '@extschema@'
AND table_name = 'pgbouncer_sockets'
AND grantee != 'PUBLIC'
GROUP BY grantee;

INSERT INTO pgbouncer_fdw_preserve_privs_temp
SELECT 'GRANT '||string_agg(privilege_type, ',')||' ON @extschema@.pgbouncer_databases TO '||grantee::text||';'
FROM information_schema.table_privileges
WHERE table_schema = '@extschema@'
AND table_name = 'pgbouncer_databases'
AND grantee != 'PUBLIC'
GROUP BY grantee;

INSERT INTO pgbouncer_fdw_preserve_privs_temp
SELECT 'GRANT '||string_agg(privilege_type, ',')||' ON @extschema@.pgbouncer_stats TO '||grantee::text||';'
FROM information_schema.table_privileges
WHERE table_schema = '@extschema@'
AND table_name = 'pgbouncer_stats'
AND grantee != 'PUBLIC'
GROUP BY grantee;

INSERT INTO pgbouncer_fdw_preserve_privs_temp
SELECT 'GRANT '||string_agg(privilege_type, ',')||' ON @extschema@.pgbouncer_users TO '||grantee::text||';'
FROM information_schema.table_privileges
WHERE table_schema = '@extschema@'
AND table_name = 'pgbouncer_users'
AND grantee != 'PUBLIC'
GROUP BY grantee;

DROP VIEW @extschema@.pgbouncer_clients;
DROP VIEW @extschema@.pgbouncer_servers;
DROP VIEW @extschema@.pgbouncer_sockets;
DROP VIEW @extschema@.pgbouncer_databases;
DROP VIEW @extschema@.pgbouncer_stats;
DROP VIEW @extschema@.pgbouncer_users;

INSERT INTO pgbouncer_fdw_preserve_privs_temp 
SELECT 'GRANT EXECUTE ON FUNCTION @extschema@.pgbouncer_clients_func()  TO '||array_to_string(array_agg('"'||grantee::text||'"'), ',')||';' 
FROM information_schema.routine_privileges
WHERE routine_schema = '@extschema@'
AND routine_name = 'pgbouncer_clients_func'
AND grantee != 'PUBLIC';

INSERT INTO pgbouncer_fdw_preserve_privs_temp 
SELECT 'GRANT EXECUTE ON FUNCTION @extschema@.pgbouncer_servers_func()  TO '||array_to_string(array_agg('"'||grantee::text||'"'), ',')||';' 
FROM information_schema.routine_privileges
WHERE routine_schema = '@extschema@'
AND routine_name = 'pgbouncer_servers_func'
AND grantee != 'PUBLIC';

INSERT INTO pgbouncer_fdw_preserve_privs_temp 
SELECT 'GRANT EXECUTE ON FUNCTION @extschema@.pgbouncer_sockets_func()  TO '||array_to_string(array_agg('"'||grantee::text||'"'), ',')||';' 
FROM information_schema.routine_privileges
WHERE routine_schema = '@extschema@'
AND routine_name = 'pgbouncer_sockets_func'
AND grantee != 'PUBLIC';

INSERT INTO pgbouncer_fdw_preserve_privs_temp
SELECT 'GRANT EXECUTE ON FUNCTION @extschema@.pgbouncer_databases_func()  TO '||array_to_string(array_agg('"'||grantee::text||'"'), ',')||';'
FROM information_schema.routine_privileges
WHERE routine_schema = '@extschema@'
AND routine_name = 'pgbouncer_databases_func'
AND grantee != 'PUBLIC';

INSERT INTO pgbouncer_fdw_preserve_privs_temp
SELECT 'GRANT EXECUTE ON FUNCTION @extschema@.pgbouncer_stats_func()  TO '||array_to_string(array_agg('"'||grantee::text||'"'), ',')||';'
FROM information_schema.routine_privileges
WHERE routine_schema = '@extschema@'
AND routine_name = 'pgbouncer_stats_func'
AND grantee != 'PUBLIC';

INSERT INTO pgbouncer_fdw_preserve_privs_temp
SELECT 'GRANT EXECUTE ON FUNCTION @extschema@.pgbouncer_users_func()  TO '||array_to_string(array_agg('"'||grantee::text||'"'), ',')||';'
FROM information_schema.routine_privileges
WHERE routine_schema = '@extschema@'
AND routine_name = 'pgbouncer_users_func'
AND grantee != 'PUBLIC';

DROP FUNCTION @extschema@.pgbouncer_clients_func();
DROP FUNCTION @extschema@.pgbouncer_servers_func();
DROP FUNCTION @extschema@.pgbouncer_sockets_func();
DROP FUNCTION @extschema@.pgbouncer_databases_func();
DROP FUNCTION @extschema@.pgbouncer_stats_func();
DROP FUNCTION @extschema@.pgbouncer_users_func();

CREATE FUNCTION @extschema@.pgbouncer_clients_func() RETURNS TABLE
( 
    pgbouncer_target_host text
    , "type" text
    , "user" text
    , database text
    , replication text
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

    IF v_version_major >= 1 AND v_version_minor >= 23 THEN
        RETURN QUERY SELECT
           v_row.target_host AS pgbouncer_target_host
           , x."type"
           , x."user"
           , x.database
           , x.replication
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
           , replication text
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
    ELSIF v_version_major = 1 AND v_version_minor >= 21 and v_version_minor < 23 THEN
        RETURN QUERY SELECT
           v_row.target_host AS pgbouncer_target_host
           , x."type"
           , x."user"
           , x.database
           , '' AS replication
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
           , '' AS replication
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
           , '' AS replication
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
    , replication text
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

    IF v_version_major >= 1 AND v_version_minor >= 23 THEN
        RETURN QUERY SELECT
            v_row.target_host AS pgbouncer_target_host
            , x."type"
            , x."user"
            , x.database
            , x.replication
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
            , replication text
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
    ELSIF v_version_major = 1 AND v_version_minor >= 21 and v_version_minor < 23 THEN
        RETURN QUERY SELECT
            v_row.target_host AS pgbouncer_target_host
            , x."type"
            , x."user"
            , x.database
            , '' AS replication
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
            , '' AS replication
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
            , '' AS replication
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
    , replication text
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

    IF v_version_major >= 1 AND v_version_minor >= 23 THEN
        RETURN QUERY SELECT
            v_row.target_host AS pgbouncer_target_host
            , x."type"
            , x."user"
            , x.database
            , x.replication
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
            , replication text
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
    ELSIF v_version_major = 1 AND v_version_minor >= 21 AND v_version_minor < 23 THEN
        RETURN QUERY SELECT
            v_row.target_host AS pgbouncer_target_host
            , x."type"
            , x."user"
            , x.database
            , '' AS replication
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
            , '' AS replication
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
            , '' AS replication
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
    , server_lifetime int
    , pool_mode text
    , max_connections int
    , current_connections int
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

    IF v_version_major >= 1 AND v_version_minor >= 23 THEN
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
            , x.server_lifetime
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
            , server_lifetime int
            , pool_mode text
            , max_connections int
            , current_connections int
            , paused int
            , disabled int
        );
    ELSIF v_version_major = 1 AND v_version_minor < 23 THEN
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
            , 0 AS server_lifetime
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


CREATE FUNCTION @extschema@.pgbouncer_stats_func() RETURNS TABLE
(
    pgbouncer_target_host text
    , database text
    , total_server_assignment_count bigint
    , total_xact_count bigint
    , total_query_count bigint
    , total_received bigint
    , total_sent bigint
    , total_xact_time bigint
    , total_query_time bigint
    , total_wait_time bigint
    , avg_server_assignment_count bigint
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

    IF v_version_major >= 1 AND v_version_minor >= 23 THEN
        RETURN QUERY SELECT
            v_row.target_host AS pgbouncer_target_host
            , x.database
            , x.total_server_assignment_count
            , x.total_xact_count
            , x.total_query_count
            , x.total_received
            , x.total_sent
            , x.total_xact_time
            , x.total_query_time
            , x.total_wait_time
            , x.avg_server_assignment_count
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
            , total_server_assignment_count bigint
            , total_xact_count bigint
            , total_query_count bigint
            , total_received bigint
            , total_sent bigint
            , total_xact_time bigint
            , total_query_time bigint
            , total_wait_time bigint
            , avg_server_assignment_count bigint
            , avg_xact_count bigint
            , avg_query_count bigint
            , avg_recv bigint
            , avg_sent bigint
            , avg_xact_time bigint
            , avg_query_time bigint
            , avg_wait_time bigint
        );
    ELSIF v_version_major = 1 AND v_version_minor < 23 THEN
        RETURN QUERY SELECT
            v_row.target_host AS pgbouncer_target_host
            , x.database
            , 0 AS total_server_assignment_count
            , x.total_xact_count
            , x.total_query_count
            , x.total_received
            , x.total_sent
            , x.total_xact_time
            , x.total_query_time
            , x.total_wait_time
            , 0 AS avg_server_assignment_count
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


CREATE FUNCTION @extschema@.pgbouncer_users_func() RETURNS TABLE
(
    pgbouncer_target_host text
    , name text
    , pool_size text
    , pool_mode text
    , max_user_connections int
    , current_connections int
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

    IF v_version_major >= 1 AND v_version_minor >= 23 THEN
        RETURN QUERY SELECT
            v_row.target_host AS pgbouncer_target_host
            , x.name
            , x.pool_size
            , x.pool_mode
            , x.max_user_connections
            , x.current_connections
        FROM dblink(v_row.target_host, 'show users') AS x
        (
            name text
            , pool_size text
            , pool_mode text
            , max_user_connections int
            , current_connections int
        );
    ELSIF v_version_major = 1 AND v_version_minor < 23 THEN
        RETURN QUERY SELECT
            v_row.target_host AS pgbouncer_target_host
            , x.name
            , 0 AS pool_size
            , x.pool_mode
            , 0 AS max_user_connections
            , 0 AS current_connections
        FROM dblink(v_row.target_host, 'show users') AS x
        (
            name text
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


CREATE VIEW @extschema@.pgbouncer_clients AS
    SELECT pgbouncer_target_host
        , "type"
        , "user"
        , database
        , replication
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
        , replication
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
        , replication
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
        , server_lifetime
        , pool_mode
        , max_connections
        , current_connections
        , paused
        , disabled
     FROM @extschema@.pgbouncer_databases_func();


CREATE VIEW @extschema@.pgbouncer_stats AS
    SELECT pgbouncer_target_host
        , database
        , total_server_assignment_count
        , total_xact_count
        , total_query_count
        , total_received
        , total_sent
        , total_xact_time
        , total_query_time
        , total_wait_time
        , avg_server_assignment_count
        , avg_xact_count
        , avg_query_count
        , avg_recv
        , avg_sent
        , avg_xact_time
        , avg_query_time
        , avg_wait_time
     FROM @extschema@.pgbouncer_stats_func();


CREATE VIEW @extschema@.pgbouncer_users AS
    SELECT pgbouncer_target_host
        , name
        , pool_size
        , pool_mode
        , max_user_connections
        , current_connections
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
