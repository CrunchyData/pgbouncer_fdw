
CREATE VIEW @extschema@.pgbouncer_clients AS
    SELECT * FROM dblink('pgbouncer', 'show clients') AS x
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
        , tls text);

CREATE VIEW @extschema@.pgbouncer_config AS
    SELECT * FROM dblink('pgbouncer', 'show config') AS x 
    (   key text
        , value text
        , changeable boolean);

CREATE VIEW @extschema@.pgbouncer_databases AS
    SELECT * FROM dblink('pgbouncer', 'show databases') AS x
    (   name text
        , host text
        , port int
        , database text
        , force_user text
        , pool_size int
        , reserve_pool int
        , pool_mode text
        , max_connections int
        , current_connections int
        , paused int
        , disabled int);

CREATE VIEW @extschema@.pgbouncer_dns_hosts AS
    SELECT * FROM dblink('pgbouncer', 'show dns_hosts') AS x
    (   hostname text
        , ttl bigint
        , addrs text);

CREATE VIEW @extschema@.pgbouncer_dns_zones AS
    SELECT * FROM dblink('pgbouncer', 'show dns_zones') AS x
    (   zonename text
        , serial text
        , count int);

CREATE VIEW @extschema@.pgbouncer_lists AS
    SELECT * FROM dblink('pgbouncer', 'show lists') AS x 
    (   list text
        , items int);

CREATE VIEW @extschema@.pgbouncer_pools AS
    SELECT * FROM dblink('pgbouncer', 'show pools') AS x
    (   database text
        , "user" text
        , cl_active int
        , cl_waiting int
        , sv_active int
        , sv_idle int
        , sv_used int
        , sv_tested int
        , sv_login int
        , maxwait int
        , maxwait_us int
        , pool_mode text);

CREATE VIEW @extschema@.pgbouncer_servers AS
    SELECT * FROM dblink('pgbouncer', 'show servers') AS x 
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
        , tls text);

CREATE VIEW @extschema@.pgbouncer_sockets AS
    SELECT * FROM dblink('pgbouncer', 'show sockets') AS x
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
        , recv_pos int
        , pkt_pos int
        , pkt_remain int
        , send_pos int
        , send_remain int
        , pkt_avail int
        , send_avail int);

CREATE VIEW @extschema@.pgbouncer_stats AS
    SELECT * FROM dblink('pgbouncer', 'show stats') AS x
    (   database text
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
        , avg_wait_time bigint );

CREATE VIEW @extschema@.pgbouncer_users AS
    SELECT * FROM dblink('pgbouncer', 'show users') AS x
    (   name text
        , pool_mode text);

