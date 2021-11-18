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

### The command is:

```bash
python readpower.py [-opts] 
```

And the options are:

```
  -h, --help            show this help message and exit
  --db DB               name of database (default:"power.db")
  --pivot               translate fact table into the usual tabular format
  --format {csv,feather,pandas,stata,parquet}
                        Output format; default is csv
  -n NODE, --node NODE  node number to investigate (default is all)
  -o OUTPUT, --output OUTPUT
                        name of output file for extracted data. A suffix 
                        will be added to reflect the data format.
  -p {c,m,t}, --point {c,m,t}
                        measurement point to consider (default is all)
  -t TIME, --time TIME  number of recent 24-hour periods to consider (default=1)
  -v, --verbose         be chatty
```

### Typical use:

[0] Source the readpower.sh file so that you don't have to type in
the word python constantly and the command becomes `readpower`.

`source readpower.py`

[1] Get a sense of how the power has been for the last day, all nodes, and 
plan to read it into Excel. Write it to a file with today's date
embedded in the name:

```bash
readpower -p t --pivot -o power.`date +%y%m%d`.csv
```
