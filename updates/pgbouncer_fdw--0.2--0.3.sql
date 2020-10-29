-- Add command functions to allow running pgBouncer commands on the target server. Note that the role defined in the user mapping must be given admin access to the pgBouncer admin console.

CREATE FUNCTION @extschema@.pgbouncer_command_disable(p_dbname text) RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM dblink_exec('pgbouncer', format('DISABLE %I', p_dbname));
END
$$;

CREATE FUNCTION @extschema@.pgbouncer_command_enable(p_dbname text) RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM dblink_exec('pgbouncer', format('ENABLE %I', p_dbname));
END
$$;

CREATE FUNCTION @extschema@.pgbouncer_command_kill(p_dbname text DEFAULT NULL) RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    IF p_dbname IS NULL THEN
        PERFORM dblink_exec('pgbouncer', 'KILL');
    ELSE
        PERFORM dblink_exec('pgbouncer', format('KILL %I', p_dbname));
    END IF;
END
$$;

CREATE FUNCTION @extschema@.pgbouncer_command_pause(p_dbname text DEFAULT NULL) RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    IF p_dbname IS NULL THEN
        PERFORM dblink_exec('pgbouncer', 'PAUSE');
    ELSE
        PERFORM dblink_exec('pgbouncer', format('PAUSE %I', p_dbname));
    END IF;
END
$$;

CREATE FUNCTION @extschema@.pgbouncer_command_reconnect(p_dbname text DEFAULT NULL) RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    IF p_dbname IS NULL THEN
        PERFORM dblink_exec('pgbouncer', 'RECONNECT');
    ELSE
        PERFORM dblink_exec('pgbouncer', format('RECONNECT %I', p_dbname));
    END IF;
END
$$;

CREATE FUNCTION @extschema@.pgbouncer_command_reload() RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM dblink_exec('pgbouncer', 'RELOAD');
END
$$;

CREATE FUNCTION @extschema@.pgbouncer_command_resume(p_dbname text DEFAULT NULL) RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    IF p_dbname IS NULL THEN
        PERFORM dblink_exec('pgbouncer', 'RESUME');
    ELSE
        PERFORM dblink_exec('pgbouncer', format('RESUME %I', p_dbname));
    END IF;
END
$$;

CREATE FUNCTION @extschema@.pgbouncer_command_set(p_name text, p_value text) RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM dblink_exec('pgbouncer', format('SET %s = %L', p_name, p_value));
END
$$;

CREATE FUNCTION @extschema@.pgbouncer_command_shutdown() RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM dblink_exec('pgbouncer', 'shutdown');
END
$$;

CREATE FUNCTION @extschema@.pgbouncer_command_suspend() RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM dblink_exec('pgbouncer', 'SUSPEND');
END
$$;

CREATE FUNCTION @extschema@.pgbouncer_command_wait_close(p_dbname text DEFAULT NULL) RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    IF p_dbname IS NULL THEN
        PERFORM dblink_exec('pgbouncer', 'WAIT_CLOSE');
    ELSE
        PERFORM dblink_exec('pgbouncer', format('WAIT_CLOSE %I', p_dbname));
    END IF;
END
$$;

REVOKE EXECUTE ON FUNCTION @extschema@.pgbouncer_command_disable(text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION @extschema@.pgbouncer_command_enable(text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION @extschema@.pgbouncer_command_kill(text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION @extschema@.pgbouncer_command_pause(text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION @extschema@.pgbouncer_command_reconnect(text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION @extschema@.pgbouncer_command_reload() FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION @extschema@.pgbouncer_command_resume(text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION @extschema@.pgbouncer_command_set(text, text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION @extschema@.pgbouncer_command_shutdown() FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION @extschema@.pgbouncer_command_suspend() FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION @extschema@.pgbouncer_command_wait_close(text) FROM PUBLIC;
