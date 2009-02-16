defaults = '''# default config
[DEFAULT]
prefix = /usr/local/numbex

[GLOBAL]
#logging_config = /etc/numbex-logging.conf
logging_config =
control_host = localhost
control_port = 44880
owner = freeconet
#private_key = /etc/numbex.pem
private_key = 

[PEER]
# multiple values allowed
trackers = http://localhost:8880
#        http://localhost:9990
user = test
auth = test

fetch_interval = 120
fetch_peers = 3

[TRACKER]
port = 8880
host =

[UDP]
# multiple values allowed
ports = 8990 8991 8992

[SOAP]
port = 8000

[GIT]
path = %(prefix)s/var/repo
publish_method = daemon
daemon_port = 11223
repo_url = git://localhost:11223/
# timeout for records exported in hours
export_timeout = 96

[DATABASE]
path = %(prefix)s/var/db/db.sqlite3
# timeout for records exported in hours
export_timeout = 96
'''
