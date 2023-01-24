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
        , application_name text
    );


CREATE VIEW @extschema@.pgbouncer_config AS
    SELECT key
          , value
          , "default"
          , changeable
    FROM dblink('pgbouncer', 'show config') AS x
    (   key text
        , value text
        , "default" text
        , changeable boolean
    );


CREATE VIEW @extschema@.pgbouncer_databases AS
    SELECT name
           , host
           , port
           , database
           , force_user
           , pool_size
           , min_pool_size
           , reserve_pool
           , pool_mode
           , max_connections
           , current_connections
           , paused
           , disabled
     FROM dblink('pgbouncer', 'show databases') AS x
    (   name text
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


CREATE VIEW @extschema@.pgbouncer_dns_hosts AS
    SELECT hostname
           , ttl
           , addrs
    FROM dblink('pgbouncer', 'show dns_hosts') AS x
    (   hostname text
        , ttl bigint
        , addrs text
    );


CREATE VIEW @extschema@.pgbouncer_dns_zones AS
    SELECT zonename
           , serial
           , count
    FROM dblink('pgbouncer', 'show dns_zones') AS x
    (   zonename text
        , serial text
        , count int
    );


CREATE VIEW @extschema@.pgbouncer_lists AS
    SELECT list
           , items
    FROM dblink('pgbouncer', 'show lists') AS x
    (   list text
        , items int
    );


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
        , pool_mode text
    );


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
        , application_name text
    );


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
        , send_avail int
    );


CREATE VIEW @extschema@.pgbouncer_stats AS
    SELECT database
           , total_xact_count
           , total_query_count
           , total_received
           , total_sent
           , total_xact_time
           , total_query_time
           , total_wait_time
           , avg_xact_count
           , avg_query_count
           , avg_recv
           , avg_sent
           , avg_xact_time
           , avg_query_time
           , avg_wait_time
    FROM dblink('pgbouncer', 'show stats') AS x
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
        , avg_wait_time bigint
    );


CREATE VIEW @extschema@.pgbouncer_users AS
    SELECT name
           , pool_mode
    FROM dblink('pgbouncer', 'show users') AS x
    (   name text
        , pool_mode text
    );
