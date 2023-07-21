-- Fix missing comma that caused missing column "pgbouncer_target_host" in "pgbouncer_servers" and "pgbouncer_dns_zones" views

CREATE TEMP TABLE pgbouncer_fdw_preserve_privs_temp (statement text);

INSERT INTO pgbouncer_fdw_preserve_privs_temp
SELECT 'GRANT '||string_agg(privilege_type, ',')||' ON @extschema@.pgbouncer_servers TO '||grantee::text||';'
FROM information_schema.table_privileges
WHERE table_schema = '@extschema@'
AND table_name = 'pgbouncer_servers'
GROUP BY grantee;

INSERT INTO pgbouncer_fdw_preserve_privs_temp
SELECT 'GRANT '||string_agg(privilege_type, ',')||' ON @extschema@.pgbouncer_dns_zones TO '||grantee::text||';'
FROM information_schema.table_privileges
WHERE table_schema = '@extschema@'
AND table_name = 'pgbouncer_dns_zones'
GROUP BY grantee;

DROP VIEW @extschema@.pgbouncer_servers;
DROP VIEW @extschema@.pgbouncer_dns_zones;

CREATE OR REPLACE VIEW @extschema@.pgbouncer_servers AS
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
     FROM @extschema@.pgbouncer_servers_func();


CREATE VIEW @extschema@.pgbouncer_dns_zones AS
    SELECT pgbouncer_target_host
        , zonename
        , serial
        , count
     FROM @extschema@.pgbouncer_dns_zones_func();

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
