CREATE TEMP TABLE pgbouncer_fdw_preserve_privs_temp (statement text);

INSERT INTO pgbouncer_fdw_preserve_privs_temp
SELECT 'GRANT '||string_agg(privilege_type, ',')||' ON @extschema@.pgbouncer_clients TO '||grantee::text||';'
FROM information_schema.table_privileges
WHERE table_schema = '@extschema@'
AND table_name = 'pgbouncer_clients'
GROUP BY grantee;

INSERT INTO pgbouncer_fdw_preserve_privs_temp
SELECT 'GRANT '||string_agg(privilege_type, ',')||' ON @extschema@.pgbouncer_pools TO '||grantee::text||';'
FROM information_schema.table_privileges
WHERE table_schema = '@extschema@'
AND table_name = 'pgbouncer_pools'
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
DROP VIEW @extschema@.pgbouncer_pools;
DROP VIEW @extschema@.pgbouncer_servers;
DROP VIEW @extschema@.pgbouncer_sockets;

CREATE VIEW @extschema@.pgbouncer_clients AS
    SELECT type
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
    FROM dblink('pgbouncer', 'show clients') AS x
    (   type text
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
        , application_name text);

CREATE VIEW @extschema@.pgbouncer_pools AS
    SELECT database
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
    FROM dblink('pgbouncer', 'show pools') AS x
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
        , pool_mode text);

CREATE VIEW @extschema@.pgbouncer_servers AS
    SELECT type
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
    FROM dblink('pgbouncer', 'show servers') AS x
    (   type text
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
        , application_name text);


CREATE VIEW @extschema@.pgbouncer_sockets AS
    SELECT type
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
    FROM dblink('pgbouncer', 'show sockets') AS x
    (   type text
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
        , send_avail int);

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
