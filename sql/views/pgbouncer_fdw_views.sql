CREATE VIEW @extschema@.pgbouncer_version AS
    SELECT pgbouncer_target_host
        ,  version_major
        , version_minor
        , version_patch
    FROM @extschema@.pgbouncer_version_func();


CREATE VIEW @extschema@.pgbouncer_clients AS
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
    FROM @extschema@.pgbouncer_clients_func();


CREATE VIEW @extschema@.pgbouncer_config AS
    SELECT pgbouncer_target_host
        , key
        , value
        , "default"
        , changeable
    FROM @extschema@.pgbouncer_config_func();


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
        , pool_mode
        , max_connections
        , current_connections
        , paused
        , disabled
     FROM @extschema@.pgbouncer_databases_func();


CREATE VIEW @extschema@.pgbouncer_dns_hosts AS
    SELECT pgbouncer_target_host
        , hostname
        , ttl
        , addrs
     FROM @extschema@.pgbouncer_dns_hosts_func();


CREATE VIEW @extschema@.pgbouncer_dns_zones AS
    SELECT pgbouncer_target_host
        zonename
        , serial
        , count
     FROM @extschema@.pgbouncer_dns_zones_func();


CREATE VIEW @extschema@.pgbouncer_lists AS
    SELECT pgbouncer_target_host
        , list
        , items
     FROM @extschema@.pgbouncer_lists_func();


CREATE VIEW @extschema@.pgbouncer_pools AS
    SELECT pgbouncer_target_host 
        , database
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


CREATE VIEW @extschema@.pgbouncer_servers AS
    SELECT pgbouncer_target_host
        "type"
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


CREATE VIEW @extschema@.pgbouncer_sockets AS
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
        , recv_pos
        , pkt_pos
        , pkt_remain
        , send_pos
        , send_remain
        , pkt_avail
        , send_avail
     FROM @extschema@.pgbouncer_sockets_func();


CREATE VIEW @extschema@.pgbouncer_stats AS
    SELECT pgbouncer_target_host
        , database
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
     FROM @extschema@.pgbouncer_stats_func();


CREATE VIEW @extschema@.pgbouncer_users AS
    SELECT pgbouncer_target_host
        , name
        , pool_mode
     FROM @extschema@.pgbouncer_users_func();


