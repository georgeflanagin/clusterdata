# veryhungrycluster
Maintain a database of power consumption on an ACT cluster

## Usage

### To start the program

`nohup python veryhungrycluster.py [-opts] >/dev/null &`

### To stop the program (gracefully)

`kill -15 PID-of-veryhungrycluster`


## Options

`-f`, `--freq` : Number of seconds between polls of the nodes in the cluster.
The default value is `300` (five minutes).

`--db` : Name of the database to write. *Note*: this database must exist ahead of time. See below.
The default name is `power.db`.

`-v` : Mainly for interactive use; this is the slightly-verbose mode.

`-n` : Limits the number of polls. For debugging.

## The database

The database is just a flying fact table, keyed on time stamp. The other 
columns represent the node name, the measurement point, and the watts consumed.

To create a new database, `sqlite3 {db name} < power.sql`


