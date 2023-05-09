-- Allow gathering statistics from multiple pgbouncer targets
    -- TODO see if exception can be caught when target it unconnectable to allow stats for working hosts


CREATE TEMP TABLE pgbouncer_fdw_preserve_privs_temp (statement text);

INSERT INTO pgbouncer_fdw_preserve_privs_temp
SELECT 'GRANT '||string_agg(privilege_type, ',')||' ON @extschema@.pgbouncer_clients TO '||grantee::text||';'
FROM information_schema.table_privileges
WHERE table_schema = '@extschema@'
AND table_name = 'pgbouncer_clients'
GROUP BY grantee;

INSERT INTO pgbouncer_fdw_preserve_privs_temp
SELECT 'GRANT '||string_agg(privilege_type, ',')||' ON @extschema@.pgbouncer_pools; TO '||grantee::text||';'
FROM information_schema.table_privileges
WHERE table_schema = '@extschema@'
AND table_name = 'pgbouncer_pools;'
GROUP BY grantee;


DROP VIEW @extschema@.pgbouncer_clients;
DROP VIEW @extschema@.pgbouncer_pools;

CREATE TABLE @extschema@.pgbouncer_fdw_targets (
    target_host text NOT NULL
    , active boolean NOT NULL DEFAULT true
    , CONSTRAINT pgbouncer_fdw_targets_pk PRIMARY KEY (target_host) );

INSERT INTO @extschema@.pgbouncer_fdw_targets ( target_host ) VALUES ('pgbouncer');

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
    v_row   record;
    v_sql   text;
BEGIN

v_sql := 'SELECT target_host FROM @extschema@.pgbouncer_fdw_targets WHERE active';
IF p_target_host IS NOT NULL THEN
    v_sql := v_sql || format(' AND target_host = %L', p_target_host);
END IF;

FOR v_row IN EXECUTE v_sql
LOOP
    RETURN QUERY SELECT 
        v_row.target_host AS pgbouncer_target_host
        , split_part(substring(version from '\d.+'), '.', 1)::int AS version_major
        , split_part(substring(version from '\d.+'), '.', 2)::int AS version_minor
        , split_part(substring(version from '\d.+'), '.', 3)::int AS version_patch
    FROM dblink(v_row.target_host, 'show version') AS x
    (   
        version text
    );
END LOOP;
END
$$;

CREATE VIEW @extschema@.pgbouncer_version AS
    SELECT version_major
            , version_minor
            , version_patch
    FROM @extschema@.pgbouncer_version_func();


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
    v_row               record;
    v_version_major     int;
    v_version_minor     int;
BEGIN

FOR v_row IN  
    SELECT target_host FROM @extschema@.pgbouncer_fdw_targets WHERE active
LOOP

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
    END IF;

END LOOP;

END
$$;

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
    FROM @extschema@.pgbouncer_clients_func();


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
    v_row       record;
    v_version_major     int;
    v_version_minor     int;
BEGIN

FOR v_row IN  
    SELECT target_host FROM @extschema@.pgbouncer_fdw_targets WHERE active
LOOP

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
    -- old cl_cancel_req is sent as cl_active_cancel_req
    ELSIF v_version_major = 1 AND v_version_minor = 17 THEN 

        RETURN QUERY SELECT 
               v_row.target_host AS pgbouncer_target_host
               , x.database
               , x."user"
               , x.cl_active
               , x.cl_waiting
               , x.cl_cancel_req AS cl_active_cancel_req
               , 0 AS cl_waiting_cancel_req
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
    END IF;

END LOOP;

END
$$;

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
    FROM @extschema@.pgbouncer_pools_func();

/*
 * FUNCTION TEMPLATE 
 *
CREATE FUNCTION @extschema@.() RETURNS TABLE 
( 
    pgbouncer_target_host text
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_row       record;
BEGIN

FOR v_row IN  
    SELECT target_host FROM @extschema@.pgbouncer_fdw_targets WHERE active
LOOP

    RETURN QUERY SELECT 
           v_row.target_host AS pgbouncer_target_host
    FROM dblink(v_row.target_host, '') AS x
    (   

    );

END LOOP;

END
$$;
****/


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

