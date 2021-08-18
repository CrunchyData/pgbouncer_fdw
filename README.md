# pgbouncer Foreign Data Wrapper

## Introduction

pgbouncer_fdw provides a direct SQL interface to the pgbouncer SHOW commands. It takes advantage of the dblink_fdw feature to provide a more typical, table-like interface to the current status of your pgbouncer server(s). This makes it easier to set up monitoring or other services that require direct access to pgbouncer statistics.

## Requirements

 * PostgreSQL 9.4+ - https://www.postgresql.org
 * dblink (contrib module) - https://www.postgresql.org/docs/current/dblink.html
 * pgbouncer 1.16+ - https://pgbouncer.github.io

## Setup

For basic monitoring of statistics, whichever database role you will be using in the user mapping below will have to be added to the `stats_users` list in the pgbouncer configuration (pgbouncer.ini). You will also need to add this role to the `auth_users` file (see NOTE below). Ensure the role used below is able to connect to the special pgbouncer database and run the SHOW commands before setting up the FDW.

For running of the command functions, that role will have to be added to the `admin_users` list in the pgbouncer configuration. It is not recommended that your monitoring role also be given admin console access. It is recommended to have a separate database role for a separate user mapping to allow access to the pgBouncer to run these commands. 

If installing from source, run make from the source directory
```
make install
```

The dblink extension must be created in a schema that is within the search path of the role that will be used for the user mapping below. A default location of the PUBLIC schema is the easiest.
```
CREATE EXTENSION dblink;
```

Create an fdw server & user mapping manually first with your preferred credentials. Leave server name as "pgbouncer". Set the port to whichever port pgbouncer itself is running on, NOT the postgres database. pgbouncer statistics are global so it only needs to be monitored from a single database. If you have multiple databases in your cluster, it is recommended to just install it to the default `postgres` database.

NOTE: The database role used for the user mapping must have an explicit entry in the pgbouncer auth_file. The auth_query method in pgbouncer cannot be used to connect to the special `pgbouncer` database where the SHOW commands must be run.

```
CREATE SERVER pgbouncer FOREIGN DATA WRAPPER dblink_fdw OPTIONS (host 'localhost',
                                                                 port '6432',
                                                                 dbname 'pgbouncer');

CREATE USER MAPPING FOR PUBLIC SERVER pgbouncer OPTIONS (user 'ccp_monitoring', password 'mypassword');
```
Optionally create a separate user mapping to allow admin command access. The example below sets the `pg_admin` role that exists in the PostgreSQL databsae to connect to the pgBouncer admin console as the role `pg_admin` which should be in the pgbouncer.ini `admin_users` list
```
CREATE USER MAPPING FOR pgb_admin SERVER pgbouncer OPTIONS (user 'pgb_admin', password 'supersecretpassword');
```
```
CREATE EXTENSION pgbouncer_fdw;
```

Grant necessary permissions on extension objects to the user mapping role
```
GRANT USAGE ON FOREIGN SERVER pgbouncer TO ccp_monitoring;

GRANT SELECT ON pgbouncer_clients TO ccp_monitoring;
GRANT SELECT ON pgbouncer_config TO ccp_monitoring;
GRANT SELECT ON pgbouncer_databases TO ccp_monitoring;
GRANT SELECT ON pgbouncer_dns_hosts TO ccp_monitoring;
GRANT SELECT ON pgbouncer_dns_zones TO ccp_monitoring;
GRANT SELECT ON pgbouncer_lists TO ccp_monitoring;
GRANT SELECT ON pgbouncer_pools TO ccp_monitoring;
GRANT SELECT ON pgbouncer_servers TO ccp_monitoring;
GRANT SELECT ON pgbouncer_sockets TO ccp_monitoring;
GRANT SELECT ON pgbouncer_stats TO ccp_monitoring;
GRANT SELECT ON pgbouncer_users TO ccp_monitoring;

```
For added security, execution on the pgBouncer command functions has been revoked from public by default. You will need to explicitly grant execute privileges on the command functions to your pgBouncer admin role if they are being used.
```
GRANT USAGE ON FOREIGN SERVER pgbouncer TO pgb_admin;

GRANT EXECUTE ON FUNCTION pgbouncer_command_disable(text) TO pgb_admin;
GRANT EXECUTE ON FUNCTION pgbouncer_command_enable(text) TO pgb_admin;
GRANT EXECUTE ON FUNCTION pgbouncer_command_kill(text) TO pgb_admin;
GRANT EXECUTE ON FUNCTION pgbouncer_command_pause(text) TO pgb_admin;
GRANT EXECUTE ON FUNCTION pgbouncer_command_reconnect(text) TO pgb_admin;
GRANT EXECUTE ON FUNCTION pgbouncer_command_reload() TO pgb_admin;
GRANT EXECUTE ON FUNCTION pgbouncer_command_resume(text) TO pgb_admin;
GRANT EXECUTE ON FUNCTION pgbouncer_command_set(text, text) TO pgb_admin;
GRANT EXECUTE ON FUNCTION pgbouncer_command_shutdown() TO pgb_admin;
GRANT EXECUTE ON FUNCTION pgbouncer_command_suspend() TO pgb_admin;
GRANT EXECUTE ON FUNCTION pgbouncer_command_wait_close(text) TO pgb_admin;
GRANT SELECT ON pgbouncer_clients TO pgb_admin;
GRANT SELECT ON pgbouncer_config TO pgb_admin;
GRANT SELECT ON pgbouncer_databases TO pgb_admin;
GRANT SELECT ON pgbouncer_dns_hosts TO pgb_admin;
GRANT SELECT ON pgbouncer_dns_zones TO pgb_admin;
GRANT SELECT ON pgbouncer_lists TO pgb_admin;
GRANT SELECT ON pgbouncer_pools TO pgb_admin;
GRANT SELECT ON pgbouncer_servers TO pgb_admin;
GRANT SELECT ON pgbouncer_sockets TO pgb_admin;
GRANT SELECT ON pgbouncer_stats TO pgb_admin;
GRANT SELECT ON pgbouncer_users TO pgb_admin;
```
## Usage
You should be able to query any of the pgbouncer views provided. For the meaning of the views, see the pgbouncer documentation (linked above). Not all views are provided either due to recommendations from author (FDS) or duplication of other view data already provided (STATS_TOTALS, STATS_AVERAGES, etc).

```
postgres=> SELECT * FROM pgbouncer_pools;
-[ RECORD 1 ]---------
database   | pgbouncer
user       | pgbouncer
cl_active  | 1
cl_waiting | 0
sv_active  | 0
sv_idle    | 0
sv_used    | 0
sv_tested  | 0
sv_login   | 0
maxwait    | 0
maxwait_us | 0
pool_mode  | statement
-[ RECORD 2 ]---------
database   | postgres
user       | postgres
cl_active  | 1
cl_waiting | 0
sv_active  | 1
sv_idle    | 0
sv_used    | 0
sv_tested  | 0
sv_login   | 0
maxwait    | 0
maxwait_us | 0
pool_mode  | session
```
