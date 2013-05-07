#!/usr/bin/env ruby
# ============================================================================
# This script is based on ss_get_mysql_stats.php 
#  from Percona Monitoring Plugins available here:
#  http://www.percona.com/software/percona-monitoring-plugins
# License: GPL License (see COPYING)
# Copyright 2013 PalominoDB Inc.
# Authors:
#  Rod Xavier Bondoc
# ============================================================================
$:.unshift File.join(File.dirname(__FILE__), *%w[.. conf])
$:.unshift File.join(File.dirname(__FILE__), *%w[.. lib])

require 'config'
require 'Sendit'
require 'optparse'
require 'rubygems'
require 'mysql'

# Defaults from config file
mysqlhost = $mysqlhost
mysqlport = $mysqlport.to_i
mysqluser = $mysqluser
mysqlpassword = $mysqlpassword
nocache = $nocache
heartbeat = $heartbeat

$cache_dir  = '/tmp'        # If set, this uses caching to avoid multiple calls.
$poll_time  = 300           # Adjust to match your polling interval.
$timezone   = nil           # If not set, uses the system default.  Example: "UTC"
$chk_options = {
    'innodb' => true,       # Do you want to check InnoDB statistics?
    'master' => true,       # Do you want to check binary logging?
    'slave' => true,        # Do you want to check slave status?
    'procs' => true,        # Do you want to check SHOW PROCESSLIST?
    'get_qrt' => true,      # Get query response times from Percona Server?
}

$debug     = false          # Define whether you want debugging behavior.
$debug_log = false          # If $debug_log is a filename, it'll be used.

# ============================================================================
# This is the main function.
# ============================================================================
def get_mysql_stats(options)
    cache_dir = $cache_dir
    poll_time = $poll_time
    chk_options = $chk_options

    # Connect to MySQL.
    host = options["mysql_host"]
    user = options["mysql_user"]
    passwd = options["mysql_password"]
    port = options["mysql_port"]
    heartbeat = options["heartbeat"]
    
    con = Mysql.new(host, user, passwd, '', port.to_i)
    
    sanitized_host = host.sub(':', '').sub('/', '_')
    sanitized_host = sanitized_host + '_' + port.to_s
    cache_file = File.join(cache_dir, "#{sanitized_host}-mysql_stats.txt")
    log_debug("Cache file is #{cache_file}")
    
    # First check the cache
    fp = nil
    if not options["nocache"]
        File.open(cache_file, 'a+') do |fp|
            locked = fp.flock(File::LOCK_SH)
            if locked
                lines = Array.new
                while line = fp.gets  
                    lines.push(line)
                end
                if File.size(cache_file) > 0 \
                        and fp.ctime.to_i + (poll_time/2) > Time.new.to_i \
                        and lines.length > 0
                    # The cache file is good to use
                    log_debug('Using the cache file')
                    return lines[0]
                else
                    log_debug('The cache file seems too small or stale')
                    locked = fp.flock(File::LOCK_EX)
                    if locked
                        if File.size(cache_file) > 0 \
                                and fp.ctime.to_i + (poll_time/2) > Time.new.to_i \
                                and lines.length > 0
                            log_debug("Using the cache file")
                            return lines[0]
                        end
                        fp.truncate(0)
                    end
                end
            else
                log_debug("Couldn't lock the cache file, ignoring it.")
                fp = nil
            end
        end
    else
        log_debug("Couldn't open cache file")
        fp = nil
    end
    
    # Set up variables
    status = { # Holds the result of SHOW STATUS, SHOW INNODB STATUS, etc
        # Define some indexes so they don't cause errors with += operations
        'relay_log_space'           => nil,
        'binary_log_space'          => nil,
        'current_transactions'      => nil,
        'locked_transactions'       => nil,
        'active_transactions'       => nil,
        'innodb_locked_tables'      => nil,
        'innodb_tables_in_use'      => nil,
        'innodb_lock_structs'       => nil,
        'innodb_lock_wait_secs'     => nil,
        'innodb_sem_waits'          => nil,
        'innodb_set_wait_time_ms'   => nil,
        # Values for the 'state' column from SHOW PROCESSLIST (converted to
        # lowercase, with spaces replaced by underscores)
        'State_closing_tables'      => nil,
        'State_copying_to_tmp_table'=> nil,
        'State_end'                 => nil,
        'State_freeing_items'       => nil,
        'State_init'                => nil,
        'State_login'               => nil,
        'State_preparing'           => nil,
        'State_reading_from_net'    => nil,
        'State_sending_data'        => nil,
        'State_sorting_result'      => nil,
        'State_statistics'          => nil,
        'State_updating'            => nil,
        'State_writing_to_net'      => nil,
        'State_none'                => nil,
        'State_other'               => nil, # Everything not listed above
    }
    
    # Get SHOW STATUS
    rs = con.query "SHOW /*!50002 GLOBAL */ STATUS"
    (0...rs.num_rows).each do
        row = rs.fetch_hash
        row = dict_change_key_case(row, 'lower')
        status[row['variable_name']] = row['value']
    end
    
    # Get SHOW VARIABLES
    rs = con.query "SHOW VARIABLES"
    (0...rs.num_rows).each do
        row = rs.fetch_hash
        row = dict_change_key_case(row, 'lower')
        status[row['variable_name']] = row['value']
    end
    
    # Get SHOW SLAVE STATUS 
    if chk_options['slave']
        rs = con.query "SHOW SLAVE STATUS"
        slave_status_row_gotten = 0
        (0...rs.num_rows).each do
            row = rs.fetch_hash
            slave_status_row_gotten += 1
            # Must lowercase keys because different MySQL versions have different
            # lettercase.
            row = dict_change_key_case(row, 'lower')
            
            status['relay_log_space'] = row['relay_log_space']
            status['slave_lag'] = row['seconds_behind_master']
            
            if heartbeat.length > 0
                rs2 = con.query "SELECT MAX(GREATEST(0, UNIX_TIMESTAMP() - UNIX_TIMESTAMP(ts) - 1)) AS delay FROM #{heartbeat}"
                slave_delay_rows_gotten = 0
                (0...rs2.num_rows).each do
                    row2 = rs2.fetch_hash
                    slave_delay_rows_gotten += 1
                    if row2.class == Hash and row2.has_key?('delay')
                        status['slave_lag'] = row2['delay']
                    else
                        log_debug("Couldn't get slave lag from %s" % (heartbeat))
                    end
                end
                
                if slave_delay_rows_gotten == 0
                    log_debug('Got nothing from SHOW SLAVE STATUS')
                end
            end
        end
    end
    
    # Get SHOW MASTER STATUS
    if chk_options['master'] and status['log_bin'] == 'ON'
        binlogs = Array.new
        rs = con.query "SHOW MASTER LOGS"
        (0...rs.num_rows).each do
            row = rs.fetch_hash
            row = dict_change_key_case(row, 'lower')
            # Older versions of MySQL may not have the File_size column in the
            # results of the command.  Zero-size files indicate the user is
            # deleting binlogs manually from disk (bad user! bad!).
            if row.has_key?('file_size') and row['file_size'].to_i > 0
                binlogs.push(row['file_size'].to_i)
            end 
        end
        
        if binlogs.length > 0
            status['binary_log_space'] = binlogs.inject{|sum,x| sum + x }
        end
    end
    
    # Get SHOW PROCESSLIST and aggregate it by state
    if chk_options['procs']
        rs = con.query "SHOW PROCESSLIST"
        (0...rs.num_rows).each do
            row = rs.fetch_hash
            state = row['state']
            if state == nil
                state = 'NULL'
            elsif state == ''
                state = 'none'
            end
            
            # MySQL 5.5 replaces the 'Locked' state with a variety of "Waiting for
            # X lock" types of statuses.  Wrap these all back into "Locked" because
            # we don't really care about the type of locking it is.
            state = state = state.sub(%r/^(Table lock|Waiting for .*lock)$/, 'Locked')
            state = state.sub(' ', '_')
            if status.has_key?("State_#{state}")
                increment(status, "State_#{state}", 1)
            else
                increment(status, 'State_other', 1)
            end
        end
    end
    
    # Get SHOW INNODB STATUS and extract the desired metrics from it
    if chk_options['innodb'] and status['have_innodb'] == 'YES'
        rs = con.query "SHOW /*!50000 ENGINE*/ INNODB STATUS"
        result = rs.fetch_hash
        istatus_text = result['Status']
        istatus_vals = get_innodb_array(istatus_text)
        
        if chk_options['get_qrt'] and status['have_response_time_distribution'] == 'YES'
            log_debug('Getting query time histogram')
            i = 0
            rs = con.query "SELECT `count`, total * 1000000 AS total FROM INFORMATION_SCHEMA.QUERY_RESPONSE_TIME WHERE `time` <> 'TOO LONG'"
            
            (0...rs.num_rows).each do
                row = rs.fetch_hash
                if i > 13
                    # It's possible that the number of rows returned isn't 14.
                    # Don't add extra status counters.
                    break
                end
                count_key = "Query_time_count_%02d" % (i)
                total_key = "Query_time_total_%02d" % (i)
                status[count_key] = row['count']
                status[total_key] = row['total']
                i += 1
            end
            
            # It's also possible that the number of rows returned is too few.
            # Don't leave any status counters unassigned; it will break graphs.
            while i <= 13
                count_key = "Query_time_count_%02d" % (i)
                total_key = "Query_time_total_%02d" % (i)
                status[count_key] = 0
                status[total_key] = 0
                i += 1
            end
        else
            log_debug('Not getting time histogram because it is not enabled')
        end
        
        # Override values from InnoDB parsing with values from SHOW STATUS,
        # because InnoDB status might not have everything and the SHOW STATUS is
        # to be preferred where possible.
        
        overrides = {
            'Innodb_buffer_pool_pages_data'  => 'database_pages',
            'Innodb_buffer_pool_pages_dirty' => 'modified_pages',
            'Innodb_buffer_pool_pages_free'  => 'free_pages',
            'Innodb_buffer_pool_pages_total' => 'pool_size',
            'Innodb_data_fsyncs'             => 'file_fsyncs',
            'Innodb_data_pending_reads'      => 'pending_normal_aio_reads',
            'Innodb_data_pending_writes'     => 'pending_normal_aio_writes',
            'Innodb_os_log_pending_fsyncs'   => 'pending_log_flushes',
            'Innodb_pages_created'           => 'pages_created',
            'Innodb_pages_read'              => 'pages_read',
            'Innodb_pages_written'           => 'pages_written',
            'Innodb_rows_deleted'            => 'rows_deleted',
            'Innodb_rows_inserted'           => 'rows_inserted',
            'Innodb_rows_read'               => 'rows_read',
            'Innodb_rows_updated'            => 'rows_updated',
        }
    
        # If the SHOW STATUS value exists, override...
        overrides.each_pair do |k, v|
            if status.has_key?(k)
                log_debug('Override #{k}')
                istatus_vals[v] = status[k]
            end
        end
        
        # Now copy the values into $status.
        istatus_vals.keys.each do |k|
            status[k] = istatus_vals[k]
        end
    end
    
    # Make table_open_cache backwards-compatible (issue 63).
    if status.has_key?('table_open_cache')
        status['table_cache'] = status['table_open_cache']
    end
    
    # Compute how much of the key buffer is used and unflushed (issue 127).
    status['Key_buf_bytes_used'] = big_sub(status['key_buffer_size'], big_multiply(status['Key_blocks_unused'], status['key_cache_block_size']))
    status['Key_bug_bytes_unflushed'] = big_multiply(status['Key_blocks_not_flushed'], status['key_cache_block_size'])
    
    if status.has_key?('unflushed_log') and status['unflushed_log']
        # TODO: I'm not sure what the deal is here; need to debug this.  But the
        # unflushed log bytes spikes a lot sometimes and it's impossible for it to
        # be more than the log buffer.
        log_debug("Unflushed log: %s" % status['unflushed_log'])
        status['unflushed_log'] = [status['unflushed_log'].to_i, status['innodb_log_buffer_size'].to_i].max
    end
    
    keys = [
        'Key_read_requests',
        'Key_reads',
        'Key_write_requests',
        'Key_writes',
        'history_list',
        'innodb_transactions',
        'read_views',
        'current_transactions',
        'locked_transactions',
        'active_transactions',
        'pool_size',
        'free_pages',
        'database_pages',
        'modified_pages',
        'pages_read',
        'pages_created',
        'pages_written',
        'file_fsyncs',
        'file_reads',
        'file_writes',
        'log_writes',
        'pending_aio_log_ios',
        'pending_aio_sync_ios',
        'pending_buf_pool_flushes',
        'pending_chkp_writes',
        'pending_ibuf_aio_reads',
        'pending_log_flushes',
        'pending_log_writes',
        'pending_normal_aio_reads',
        'pending_normal_aio_writes',
        'ibuf_inserts',
        'ibuf_merged',
        'ibuf_merges',
        'spin_waits',
        'spin_rounds',
        'os_waits',
        'rows_inserted',
        'rows_updated',
        'rows_deleted',
        'rows_read',
        'Table_locks_waited',
        'Table_locks_immediate',
        'Slow_queries',
        'Open_files',
        'Open_tables',
        'Opened_tables',
        'innodb_open_files',
        'open_files_limit',
        'table_cache',
        'Aborted_clients',
        'Aborted_connects',
        'Max_used_connections',
        'Slow_launch_threads',
        'Threads_cached',
        'Threads_connected',
        'Threads_created',
        'Threads_running',
        'max_connections',
        'thread_cache_size',
        'Connections',
        'slave_running',
        'slave_stopped',
        'Slave_retried_transactions',
        'slave_lag',
        'Slave_open_temp_tables',
        'Qcache_free_blocks',
        'Qcache_free_memory',
        'Qcache_hits',
        'Qcache_inserts',
        'Qcache_lowmem_prunes',
        'Qcache_not_cached',
        'Qcache_queries_in_cache',
        'Qcache_total_blocks',
        'query_cache_size',
        'Questions',
        'Com_update',
        'Com_insert',
        'Com_select',
        'Com_delete',
        'Com_replace',
        'Com_load',
        'Com_update_multi',
        'Com_insert_select',
        'Com_delete_multi',
        'Com_replace_select',
        'Select_full_join',
        'Select_full_range_join',
        'Select_range',
        'Select_range_check',
        'Select_scan',
        'Sort_merge_passes',
        'Sort_range',
        'Sort_rows',
        'Sort_scan',
        'Created_tmp_tables',
        'Created_tmp_disk_tables',
        'Created_tmp_files',
        'Bytes_sent',
        'Bytes_received',
        'innodb_log_buffer_size',
        'unflushed_log',
        'log_bytes_flushed',
        'log_bytes_written',
        'relay_log_space',
        'binlog_cache_size',
        'Binlog_cache_disk_use',
        'Binlog_cache_use',
        'binary_log_space',
        'innodb_locked_tables',
        'innodb_lock_structs',
        'State_closing_tables',
        'State_copying_to_tmp_table',
        'State_end',
        'State_freeing_items',
        'State_init',
        'State_locked',
        'State_login',
        'State_preparing',
        'State_reading_from_net',
        'State_sending_data',
        'State_sorting_result',
        'State_statistics',
        'State_updating',
        'State_writing_to_net',
        'State_none',
        'State_other',
        'Handler_commit',
        'Handler_delete',
        'Handler_discover',
        'Handler_prepare',
        'Handler_read_first',
        'Handler_read_key',
        'Handler_read_next',
        'Handler_read_prev',
        'Handler_read_rnd',
        'Handler_read_rnd_next',
        'Handler_rollback',
        'Handler_savepoint',
        'Handler_savepoint_rollback',
        'Handler_update',
        'Handler_write',
        'innodb_tables_in_use',
        'innodb_lock_wait_secs',
        'hash_index_cells_total',
        'hash_index_cells_used',
        'total_mem_alloc',
        'additional_pool_alloc',
        'uncheckpointed_bytes',
        'ibuf_used_cells',
        'ibuf_free_cells',
        'ibuf_cell_count',
        'adaptive_hash_memory',
        'page_hash_memory',
        'dictionary_cache_memory',
        'file_system_memory',
        'lock_system_memory',
        'recovery_system_memory',
        'thread_hash_memory',
        'innodb_sem_waits',
        'innodb_sem_wait_time_ms',
        'Key_buf_bytes_unflushed',
        'Key_buf_bytes_used',
        'key_buffer_size',
        'Innodb_row_lock_time',
        'Innodb_row_lock_waits',
        'Query_time_count_00',
        'Query_time_count_01',
        'Query_time_count_02',
        'Query_time_count_03',
        'Query_time_count_04',
        'Query_time_count_05',
        'Query_time_count_06',
        'Query_time_count_07',
        'Query_time_count_08',
        'Query_time_count_09',
        'Query_time_count_10',
        'Query_time_count_11',
        'Query_time_count_12',
        'Query_time_count_13',
        'Query_time_total_00',
        'Query_time_total_01',
        'Query_time_total_02',
        'Query_time_total_03',
        'Query_time_total_04',
        'Query_time_total_05',
        'Query_time_total_06',
        'Query_time_total_07',
        'Query_time_total_08',
        'Query_time_total_09',
        'Query_time_total_10',
        'Query_time_total_11',
        'Query_time_total_12',
        'Query_time_total_13',
    ]
    
    # Return the output.
    output = Array.new
    keys.each do |k|
        # If the value isn't defined, return -1 which is lower than (most graphs')
        # minimum value of 0, so it'll be regarded as a missing value.
        val = status[k] != nil ? status[k] : -1
        output.push("#{k}:#{val}")
    end
    
    result = output.join(' ')
    if fp != nil
        File.open(cache_file, 'w+') do |fp|
            fp.puts("#{result}\n")
        end
    end
    
    con.close
    return result
end

# ============================================================================
# A drop-in replacement for PHP's array_change_key_case
# ============================================================================
def dict_change_key_case(input, key_case)
    case_lower = 'lower'
    case_upper = 'upper'
    if key_case == case_lower
        f = lambda { |x| x.downcase }
    elsif key_case == case_upper
        f = lambda { |x| x.upcase }
    else
        raise Exception
    end
    
    hash = Hash.new
    input.each_pair do |k, v|
        hash[f[k]] = v
    end
    return hash
end

# ============================================================================
# A drop-in replacement for PHP's base_convert    
# ============================================================================
def base_convert(number, fromBase, toBase)
    # Convert number to base 10
    base10 = number.to_i(fromBase)
    
    if toBase < 2 or toBase > 36
        raise NotImplementedError
    end
    
    output_value = ''
    digits = '0123456789abcdefghijklmnopqrstuvwxyz'
    digits = digits.split(//)
    sign = ''
    
    if base10 == 0
        return '0'
    elsif base10 < 0
        sign = '-'
        base10 = -base10
    end
    
    # Convert to base toBase
    s = ''
    while base10 != 0
        r = base10 % toBase
        r = r.to_i
        s = digits[r] + s
        base10 = base10 / toBase
    end
    
    output_value = sign + s
    return output_value
end

# ============================================================================
# Given INNODB STATUS text, returns a dictionary of the parsed text.  Each
# line shows a sample of the input for both standard InnoDB as you would find in
# MySQL 5.0, and XtraDB or enhanced InnoDB from Percona if applicable.  Note
# that extra leading spaces are ignored due to trim().
# ============================================================================        
def get_innodb_array(text)
    result = {
        'spin_waits'                => Array.new,
        'spin_rounds'               => Array.new,
        'os_waits'                  => Array.new,
        'pending_normal_aio_reads'  => nil,
        'pending_normal_aio_writes' => nil,
        'pending_ibuf_aio_reads'    => nil,
        'pending_aio_log_ios'       => nil,
        'pending_aio_sync_ios'      => nil,
        'pending_log_flushes'       => nil,
        'pending_buf_pool_flushes'  => nil,
        'file_reads'                => nil,
        'file_writes'               => nil,
        'file_fsyncs'               => nil,
        'ibuf_inserts'              => nil,
        'ibuf_merged'               => nil,
        'ibuf_merges'               => nil,
        'log_bytes_written'         => nil,
        'unflushed_log'             => nil,
        'log_bytes_flushed'         => nil,
        'pending_log_writes'        => nil,
        'pending_chkp_writes'       => nil,
        'log_writes'                => nil,
        'pool_size'                 => nil,
        'free_pages'                => nil,
        'database_pages'            => nil,
        'modified_pages'            => nil,
        'pages_read'                => nil,
        'pages_created'             => nil,
        'pages_written'             => nil,
        'queries_inside'            => nil,
        'queries_queud'             => nil,
        'read_views'                => nil,
        'rows_inserted'             => nil,
        'rows_updated'              => nil,
        'rows_deleted'              => nil,
        'rows_read'                 => nil,
        'innodb_transactions'       => nil,
        'unpurged_txns'             => nil,
        'history_list'              => nil,
        'current_transactions'      => nil,
        'hash_index_cells_total'    => nil,
        'hash_index_cells_used'     => nil,
        'total_mem_alloc'           => nil,
        'additional_pool_alloc'     => nil,
        'last_checkpoint'           => nil,
        'uncheckpointed_bytes'      => nil,
        'ibuf_used_cells'           => nil,
        'ibuf_free_cells'           => nil,
        'ibuf_cell_count'           => nil,
        'adaptive_hash_memory'      => nil,
        'page_hash_memory'          => nil,
        'dictionary_cache_memory'   => nil,
        'file_system_memory'        => nil,
        'lock_system_memory'        => nil,
        'recovery_system_memory'    => nil,
        'thread_hash_memory'        => nil,
        'innodb_sem_waits'          => nil,
        'innodb_sem_wait_time_ms'   => nil,
    }
    txn_seen = false
    prev_line = ''
    text.split("\n").each do |line|
        line =  line.strip
        row = line.split(%r/ +/)
        # SEMAPHORES
        if line.index('Mutex spin waits') == 0
            # Mutex spin waits 79626940, rounds 157459864, OS waits 698719
            # Mutex spin waits 0, rounds 247280272495, OS waits 316513438
            result['spin_waits'].push(to_int(row[3]))
            result['spin_rounds'].push(to_int(row[5]))
        elsif line.index('RW-shared spins') == 0 and line.index(';') != nil and line.index(';') > 0
            # RW-shared spins 3859028, OS waits 2100750; RW-excl spins 4641946, OS waits 1530310
            result['spin_waits'].push(to_int(row[2]))
            result['spin_waits'].push(to_int(row[8]))
            result['os_waits'].push(to_int(row[5]))
            result['os_waits'].push(to_int(row[11]))
        elsif line.index('RW-shared spins') == 0 and line.index('; RW-excl spins') == nil
            # Post 5.5.17 SHOW ENGINE INNODB STATUS syntax
            # RW-shared spins 604733, rounds 8107431, OS waits 241268
            result['spin_waits'].push(to_int(row[2]))
            result['os_waits'].push(to_int(row[7]))
        elsif line.index('RW-excl spins') == 0
            # Post 5.5.17 SHOW ENGINE INNODB STATUS syntax
            # RW-excl spins 604733, rounds 8107431, OS waits 241268
            result['spin_waits'].push(to_int(row[2]))
            result['os_waits'].push(to_int(row[7]))
        elsif line.index('seconds the semaphore') != nil and line.index('seconds the semaphore:') > 0
            # --Thread 907205 has waited at handler/ha_innodb.cc line 7156 for 1.00 seconds the semaphore:
            increment(result, 'innodb_sem_waits', 1)
            increment(result, 'innodb_sem_wait_time_ms', to_int(row[9])*1000)
            
        # TRANSACTIONS
        elsif line.index('Trx id counter') == 0
            # The beginning of the TRANSACTIONS section: start counting
            # transactions
            # Trx id counter 0 1170664159
            # Trx id counter 861B144C
            begin
                val = make_bigint(row[3], row[4])
            rescue
                val = make_bigint(row[3], nil)
            end
            result['innodb_transactions'] = val
            txn_seen = true
        elsif line.index('Purge done for trx') == 0
            # Purge done for trx's n:o < 0 1170663853 undo n:o < 0 0
            # Purge done for trx's n:o < 861B135D undo n:o < 0
            purged_to = make_bigint(row[6], row[7] == 'undo' ? nil : row[7])
            result['unpurged_txns'] = big_sub(result['innodb_transactions'], purged_to)
        elsif line.index('History list length') == 0
            # History list length 132
            result['history_list'] = to_int(row[3])
        elsif txn_seen and line.index('---TRANSACTION') == 0
            # ---TRANSACTION 0, not started, process no 13510, OS thread id 1170446656
            if line.index('ACTIVE') != nil and line.index('ACTIVE') > 0
                increment(result, 'active_transactions', 1)
            end
        elsif txn_seen and line.index('------- TRX HAS BEEN') == 0
            # ------- TRX HAS BEEN WAITING 32 SEC FOR THIS LOCK TO BE GRANTED:
            increment(result, 'innodb_lock_wait_secs', to_int(row[5]))
        elsif line.index('read views open inside InnoDB') != nil and line.index('read views open inside InnoDB') > 0
            # 1 read views open inside InnoDB
            result['read_views'] = to_int(row[0])
        elsif line.index('mysql tables in use') == 0
            # mysql tables in use 2, locked 2
            increment(result, 'innodb_tables_in_use', to_int(row[4]))
            increment(result, 'innodb_locked_tables', to_int(row[6]))
        elsif txn_seen and line.index('lock struct(s)') != nil and line.index('lock struct(s)') > 0
            # 23 lock struct(s), heap size 3024, undo log entries 27
            # LOCK WAIT 12 lock struct(s), heap size 3024, undo log entries 5
            # LOCK WAIT 2 lock struct(s), heap size 368
            if line.index('LOCK WAIT') == 0
                increment(result, 'innodb_lock_structs', to_int(row[2]))
                increment(result, 'locked_transactions', 1)
            else
                increment(result, 'innodb_lock_structs', to_int(row[0]))
            end
            
        # FILE I/O
        elsif line.index(' OS file reads, ') != nil and line.index(' OS file reads, ') > 0
            # 8782182 OS file reads, 15635445 OS file writes, 947800 OS fsyncs
            result['file_reads'] = to_int(row[0])
            result['file_writes'] = to_int(row[4])
            result['file_fsyncs'] = to_int(row[8])
        elsif line.index('Pending normal aio reads:') == 0
            # Pending normal aio reads: 0, aio writes: 0,
            result['pending_normal_aio_reads'] = to_int(row[4])
            result['pending_normal_aio_writes'] = to_int(row[7])
        elsif line.index('ibuf aio reads') == 0
            # ibuf aio reads: 0, log i/o's: 0, sync i/o's: 0
            result['pending_ibuf_aio_reads'] = to_int(row[3])
            result['pending_aio_log_ios'] = to_int(row[6])
            result['pending_aio_sync_ios'] = to_int(row[9])
        elsif line.index('Pending flushes (fsync)') == 0
            # Pending flushes (fsync) log: 0; buffer pool: 0
            result['pending_log_flushes'] = to_int(row[4])
            result['pending_buf_pool_flushes'] = to_int(row[7])
            
        # INSERT BUFFER AND ADAPTIVE HASH INDEX
        elsif line.index('Ibuf for space 0: size ') == 0
            # Older InnoDB code seemed to be ready for an ibuf per tablespace.  It
            # had two lines in the output.  Newer has just one line, see below.
            # Ibuf for space 0: size 1, free list len 887, seg size 889, is not empty
            # Ibuf for space 0: size 1, free list len 887, seg size 889,
            result['ibuf_used_cells'] = to_int(row[5])
            result['ibuf_free_cells'] = to_int(row[9])
            result['ibuf_cell_count'] = to_int(row[12])
        elsif line.index('Ibuf: size') == 0
            # Ibuf: size 1, free list len 4634, seg size 4636,
            result['ibuf_used_cells'] = to_int(row[2])
            result['ibuf_free_cells'] = to_int(row[6])
            result['ibuf_cell_count'] = to_int(row[9])
            if line.index('merges') != nil:
                result['ibuf_merges'] = to_int(row[10])
            end
        elsif line.index('delete mark ') != nil and line.index('delete mark ') > 0 and prev_line.index('merged operations:') == 0
            # Output of show engine innodb status has changed in 5.5
            # merged operations:
            # insert 593983, delete mark 387006, delete 73092
            result['ibuf_inserts'] = to_int(row[1])
            result['ibuf_merged'] = to_int(row[1]) + to_int(row[4]) + to_int(row[6])
        elsif line.index('merged recs, ') != nil and line.index('merged recs, ') > 0
            # 19817685 inserts, 19817684 merged recs, 3552620 merges
            result['ibuf_inserts'] = to_int(row[0])
            result['ibuf_merged'] = to_int(row[2])
            result['ibuf_merges'] = to_int(row[5])
        elsif line.index('Hash table size ') == 0
            # In some versions of InnoDB, the used cells is omitted.
            # Hash table size 4425293, used cells 4229064, ....
            # Hash table size 57374437, node heap has 72964 buffer(s) <-- no used cells
            result['hash_index_cells_total'] = to_int(row[3])
            result['hash_index_cells_used'] = line.index('used cells') != nil and line.index('used cells') > 0 ? to_int(row[6]) : '0'
            
        # LOG
        elsif line.index(' log i/o\'s done, ') != nil and line.index(' log i/o\'s done, ') > 0
            # 3430041 log i/o's done, 17.44 log i/o's/second
            # 520835887 log i/o's done, 17.28 log i/o's/second, 518724686 syncs, 2980893 checkpoints
            # TODO: graph syncs and checkpoints
            result['log_writes'] = to_int(row[0])
        elsif line.index(' pending log writes, ') != nil and line.index(' pending log writes, ') > 0
            # 0 pending log writes, 0 pending chkp writes
            result['pending_log_writes'] = to_int(row[0])
            result['pending_chkp_writes'] = to_int(row[4])
        elsif line.index('Log sequence number') == 0
            # This number is NOT printed in hex in InnoDB plugin.
            # Log sequence number 13093949495856 //plugin
            # Log sequence number 125 3934414864 //normal
            begin
                val = make_bigint(row[3], row[4])
            rescue
                val = to_int(row[3])
            end
            result['log_bytes_written'] = val
        elsif line.index('Log flushed up to') == 0
            # This number is NOT printed in hex in InnoDB plugin.
            # Log flushed up to   13093948219327
            # Log flushed up to   125 3934414864
            begin
                val = make_bigint(row[4], row[5])
            rescue
                val = to_int(row[4])
            end
            result['log_bytes_flushed'] = val
        elsif line.index('Last checkpoint at') == 0
            # Last checkpoint at  125 3934293461
            begin
                val = make_bigint(row[3], row[4])
            rescue
                val = to_int(row[3])
            end
            result['last_checkpoint'] = val
        
        # BUFFER POOL AND MEMORY
        elsif line.index('Total memory allocated') == 0
            # Total memory allocated 29642194944; in additional pool allocated 0
            result['total_mem_alloc'] = to_int(row[3])
            result['additional_pool_alloc'] = to_int(row[8])
        elsif line.index('Adaptive hash index ') == 0
            #   Adaptive hash index 1538240664 	(186998824 + 1351241840)
            result['adaptive_hash_memory'] = to_int(row[3])
        elsif line.index('Page hash           ') == 0
            #   Page hash           11688584
            result['page_hash_memory'] = to_int(row[2])
        elsif line.index('Dictionary cache    ') == 0
            #   Dictionary cache    145525560 	(140250984 + 5274576)
            result['dictionary_cache_memory'] = to_int(row[2])
        elsif line.index('File system         ') == 0
            #   File system         313848 	(82672 + 231176)
            result['file_system_memory'] = to_int(row[2])
        elsif line.index('Lock system         ') == 0
            #   Lock system         29232616 	(29219368 + 13248)
            result['lock_system_memory'] = to_int(row[2])
        elsif line.index('Recovery system     ') == 0
            #   Recovery system     0 	(0 + 0)
            result['recovery_system_memory'] = to_int(row[2])
        elsif line.index('Threads             ') == 0
            #   Threads             409336 	(406936 + 2400)
            result['thread_hash_memory'] = to_int(row[1])
        elsif line.index('Buffer pool size ') == 0
            # The " " after size is necessary to avoid matching the wrong line:
            # Buffer pool size        1769471
            # Buffer pool size, bytes 28991012864
            result['pool_size'] = to_int(row[3])
        elsif line.index('Free buffers') == 0
            # Free buffers            0
            result['free_pages'] = to_int(row[2])
        elsif line.index('Database pages') == 0
            # Database pages          1696503
            result['database_pages'] = to_int(row[2])
        elsif line.index('Modified db pages') == 0
            # Modified db pages       160602
            result['modified_pages'] = to_int(row[3])
        elsif line.index('Pages read ahead') == 0
            # Must do this BEFORE the next test, otherwise it'll get fooled by this
            # line from the new plugin (see samples/innodb-015.txt):
            # Pages read ahead 0.00/s, evicted without access 0.06/s
            # TODO: No-op for now, see issue 134.
        elsif line.index('Pages read') == 0
            # Pages read 15240822, created 1770238, written 21705836
            result['pages_read'] = to_int(row[2])
            result['pages_created'] = to_int(row[4])
            result['pages_written'] = to_int(row[6])
            
        # ROW OPERATIONS
        elsif line.index('Number of rows inserted') == 0
            # Number of rows inserted 50678311, updated 66425915, deleted 20605903, read 454561562
            result['rows_inserted'] = to_int(row[4])
            result['rows_updated'] = to_int(row[6])
            result['rows_deleted'] = to_int(row[8])
            result['rows_read'] = to_int(row[10])
        elsif line.index(' queries inside InnoDB, ') != nil and line.index(' queries inside InnoDB, ') > 0
            # 0 queries inside InnoDB, 0 queries in queue
            result['queries_inside'] = to_int(row[0])
            result['queries_queued'] = to_int(row[4])
        end
        
        prev_line = line
    end
    
    ['spin_waits', 'spin_rounds', 'os_waits'].each do |key|
        result[key] = result[key].inject{|sum,x| sum + x }
    end
    
    result['unflushed_log'] = big_sub(result['log_bytes_written'], result['log_bytes_flushed'])
    result['uncheckpointed_bytes'] = big_sub(result['log_bytes_written'], result['last_checkpoint'])
    
    return result
end

# ============================================================================
# Returns a bigint from two ulint or a single hex number.
# ============================================================================
def make_bigint(hi, lo)
    log_debug([hi, lo])
    if lo == nil
        return base_convert(hi, 16, 10)
    else
        hi = hi ? hi : '0'
        lo = lo ? lo : '0'
        return big_add(big_multiply(hi, 4294967296), lo)
    end
end

# ============================================================================
# Extracts the numbers from a string.
# ============================================================================
def to_int(val)
    log_debug(val)
    m = val.match(/\d+/)
    if m != nil
        val = m.to_s.to_i
    else
        val = 0
    end
    return val
end

# ============================================================================
# Safely increments a value that might be null.
# ============================================================================
def increment(hash, key, how_much)
    log_debug([key, how_much])
    if hash.key?(key) and hash[key] != nil
        hash[key] = big_add(hash[key], how_much)
    else
        hash[key] = how_much
    end
end

# ============================================================================
# Multiply two big integers together
# ============================================================================
def big_multiply(left, right)
    begin
        left = left.to_i
    rescue
        left = 0
    end
    begin
        right = right.to_i
    rescue
        right = 0
    end
    return left * right
end

# ============================================================================
# Subtract two big integers
# ============================================================================
def big_sub(left, right)
    begin
        left = left.to_i
    rescue
        left = 0
    end
    begin
        right = right.to_i
    rescue
        right = 0
    end
    return left - right
end

# ============================================================================
# Add two big integers together
# ============================================================================
def big_add(left, right)
    begin
        left = left.to_i
    rescue
        left = 0
    end
    begin
        right = right.to_i
    rescue
        right = 0
    end
    return left + right
end
    
# ============================================================================
# Writes to a debugging log.
# ============================================================================
def log_debug(val)
    debug = $debug
    debug_log = $debug_log
    if not debug and not debug_log
        return
    end
    begin
        trace = caller
        trace_str = trace.join(' <- ')
        t1 = Time.new
        File.open(debug_log, 'a+') do |f|
            f.puts "#{t1}: #{trace_str}"
            f.puts val.inspect
        end
    rescue Exception => e
        puts e
        puts 'Warning: disabling debug logging to #{debug_log}' 
        $debug_log = false
    end
end

# ============================================================================
# Main
# ============================================================================
options = {}
OptionParser.new do |opts|
    opts.banner = "Usage: GetMySqlStats.rb [options]"  
    
    options["mysql_host"] = mysqlhost
    opts.on("--host HOST", "MySQL host (default: #{mysqlhost})") do |v|
        options["mysql_host"] = v
    end
    
    options["mysql_port"] = mysqlport
    opts.on("--port PORT", Integer, "MySQL port (default: #{mysqlport})") do |v|
        options["mysql_port"] = v
    end
    
    options["mysql_user"] = mysqluser
    opts.on("--user USER", "MySQL username (default: #{mysqluser})") do |v|
        options["mysql_user"] = v
    end
    
    options["mysql_password"] = mysqlpassword
    opts.on("--password PASSWORD", "MySQL password (default: #{mysqlpassword})") do |v|
        options["mysql_password"] = v
    end
    
    options["heartbeat"] = heartbeat
    opts.on("--heartbeat HEARTBEAT", "MySQL heartbeat table (see pt-heartbeat) (default: )") do |v|
        options["heartbeat"] = v
    end
    
    options["nocache"] = nocache
    opts.on("--nocache", "Do not cache results in a file (default: #{nocache})") do |v|
        options["nocache"] = v
    end
    
    opts.on_tail("-h", "--help", "Show this message") do
        puts opts
        exit
    end
end.parse!

log_debug(options)

result = get_mysql_stats(options)
log_debug(result)

output = Array.new
unix_ts = Time.new.to_i
sanitized_host = options['mysql_host'].sub(':', '').sub('.', '').sub('/', '_')
sanitized_host = sanitized_host + '_' + options['mysql_port'].to_s

result.split.each do |stat|
    var_name, val = stat.split(':')
    output.push("mysql.#{sanitized_host}.#{var_name} #{val} #{unix_ts}")
end
output = output.uniq
log_debug(['Final result', output])
# Send to graphs
output.each do |metric|
    metricpath, metricvalue, metrictimestamp = metric.split
    if metricvalue.to_i >= 0
        Sendit metricpath, metricvalue, metrictimestamp
    end
end
