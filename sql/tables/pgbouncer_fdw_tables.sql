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
