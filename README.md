# veryhungrycluster and readpower
Maintain a database of power consumption on an ACT cluster,
and make it easy to extract interesting information about
it by node, time, and/or measurement point.

## Usage

### To start the program

`nohup python veryhungrycluster.py [-opts] >/dev/null &`

### To stop the program (gracefully)

`kill -SIGUSR1 PID-of-veryhungrycluster`


### Options

`-f`, `--freq` : Number of seconds between polls of the nodes in the cluster.
The default value is `300` (five minutes).

`--db` : Name of the database to write. *Note*: this database must exist ahead of time. See below.
The default name is `power.db`.

`-v` : Mainly for interactive use; this is the slightly-verbose mode.

`-n` : Limits the number of polls. For debugging.

## The database

The database is just a flying fact table, ordered (but not keyed) by time stamp. The other 
columns represent the node name, the measurement point, and the watts consumed.

To create a new database, `sqlite3 {db name} < power.sql`



## Reading the database.

The command is:

```bash
python readpower.py [-opts] 
```

And the options are:

`--db` -- name of database (default:"power.db")

`--format` "csv" or "pandas" output.

`-n NODE`, `--node NODE`  node number to investigate (default is all),
    and multiple nodes may be named.

`-o OUTPUT`, `--output OUTPUT` name of output file for extracted data.
    The default value is facts.csv in the current directory.

`-p {c,m,t}`, `--point {c,m,t}` measurement point to consider (default is all),
    and `c` is CPU, `m` is memory, and `t` is the total for the node.

`-t TIME`, `--time TIME`  number of recent 24-hour periods to consider (default=1).
    For example, if you want to look at the last week, `-t 7`

`-v`, `--verbose` This will print the SQL statement and the resolved
    command line arguments.

