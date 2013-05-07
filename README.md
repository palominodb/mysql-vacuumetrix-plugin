Vacuumetrix plugins
================================

1. MySQL.rb
-------------------------------
*A native ruby script for getting mySQL stats*

Usage:

        Usage: GetMySqlStats.rb [options]
            --host HOST                  MySQL host (default: localhost)
            --port PORT                  MySQL port (default: 3306)
            --user USER                  MySQL username (default: )
            --password PASSWORD          MySQL password (default: )
            --heartbeat HEARTBEAT        MySQL heartbeat table (see pt-heartbeat) (default: )
            --nocache                    Do not cache results in a file (default: false)
        -h, --help                       Show this message

Dependencies:

    gem install mysql

1. Move the script to vacuumetrix's bin directory

        mv GetMySqlStats.rb path/to/vacuumetrix/bin/

2. Add and configure the following to the vacuumetrix config file. These are used as default values.
    
        $nocache = false
        $heartbeat = ''
        $mysqlhost = 'localhost'
        $mysqlport = 3306
        $mysqluser = ''
        $mysqlpassword = ''

3. Add it to the crontab.

        *   *   *   *   *   /path/to/vacuumetrix/bin/MySQL.rb
