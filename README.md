## Setup
If installing from source, run make from with source directory
```
make install
```

The dblink extension must be created in a schema that is within the search path of the role that will be used for the user mapping below. A default location of the PUBLIC schema is the easiest.
```
CREATE EXTENSION dblink;
```

Create an fdw server & user mapping manually first with your preferred credentials. Leave server name as "pgbouncer". pgbouncer statistics are global so it only needs to be monitored from a single database. If you have multiple databases in your cluster, it is recommended to just install it to the default `postgres` database.
```
CREATE SERVER pgbouncer FOREIGN DATA WRAPPER dblink_fdw OPTIONS (host 'localhost',
                                                                 port '5432',
                                                                 dbname 'pgbouncer');

CREATE USER MAPPING FOR PUBLIC SERVER pgbouncer OPTIONS (user 'ccp_monitoring', password 'mypassword');
```

Recommend placing this extension's objects in their own dedicated schema
```
CREATE SCHEMA pgbouncer;

CREATE EXTENSION pgbouncer_fdw SCHEMA pgbouncer;
```

Grant necessary permissions on extension objects to the user mapping role
```
GRANT USAGE ON FOREIGN SERVER pgbouncer TO ccp_monitoring;

GRANT USAGE ON SCHEMA pgbouncer TO ccp_monitoring;

GRANT SELECT ON ALL TABLES IN SCHEMA pgbouncer TO ccp_monitoring;
```
