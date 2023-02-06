# -*- coding: utf-8 -*-

import os
import sys

# Standard imports.

import argparse
import json
import random
import shutil
import signal
import time

# Imports from UR's hpclib.

from   dorunrun import dorunrun, ExitCode
import linuxutils
from   sqlitedb import SQLiteDB
from   urdecorators import show_exceptions_and_frames as trap

###
# Credits
###
__author__ = 'George Flanagin'
__copyright__ = 'Copyright 2021'
__credits__ = None
__version__ = 1.0
__maintainer__ = 'George Flanagin'
__email__ = ['me@georgeflanagin.com', 'gflanagin@richmond.edu']
__status__ = 'Early production'
__license__ = 'MIT'

#################################################
# This program has a lot of static data
#################################################

caught_signals = [  signal.SIGINT, signal.SIGQUIT,
                    signal.SIGUSR1, signal.SIGUSR2, signal.SIGTERM ]

# This is the ACT program that gathers data from nodes.
cv_stats = shutil.which('cv-stats')
sinfo = shutil.which('sinfo')
# Request the info in JSON.
exe_statements={
    'node_stats' : f"{cv_stats} --nodes {{}} --format json",
    'node_list' : f"{sinfo} -o %n" 
    }

# There is only one thing we do; insert a few rows.
sql_statements = {
    'watts', """INSERT INTO FACTS (t, node, point, watts) VALUES (?, ?, ?, ?)""",
    'temps', """INSERT INTO TEMPS (node, air_in, air_out) VALUES (?, ?, ?)"""
    }

# These are what we search for in the JSON blob.
wattage_keys = (
    'power.cpu_watts', 'power.memory_watts', 'power.node_watts'
    )

temperature_keys = (
    'temperature[device=Front Panel Temp,unit=degrees C].temperature_value',
    'temperature[device=Exit Air Temp,unit=degrees C].temperature_value'
    )

# Shorter names in the database.
db_keys = ('c', 'm', 't') 
db_names = dict(zip(wattage_keys, db_keys))


# CREATE TABLE if not exists temps (
#     t INTEGER,
#     node INTEGER,
#     air_in FLOAT,
#     air_out FLOAT
#     );

db_temp_keys = ('air_in', 'air_out')
db_temp_names = dict(zip(temperature_keys, db_temp_keys)

##################################################
# There are only two globals, and this one is needed
# by the signal handler that, otherwise, cannot
# access it.
##################################################
db_handle = None

##################################################
# Intercept signals and die gracefully, unlike
# Hamlet in Act V. 
##################################################
def handler(signum:int, stack:object=None) -> None:
    """
    Universal signal handler.
    """
    if signum == signal.SIGHUP:
        return

    if signum in caught_signals:
        try:
            db_handle.commit()
            db_handle.db.close()
        except Exception as e:
            sys.stderr.write(f"Error on exit {e}\n")
            sys.exit(os.EX_IOERR)
        else:
            sys.exit(os.EX_OK)
    else:
        return


def collect_data(db:object, node_dict:dict) -> int:
    """
    Use ACT's tools to poll the nodes and write the fact table
    of the database.
    """
    global exe_statement
    blob=json.loads(dorunrun(
        exe_statement, return_datatype=str).strip())

    #########################################################
    # The times for reading each node are not significantly
    # different from each other, so let's go with "now." 
    #########################################################
    t = int(time.time())

    #########################################################
    # Use a dict comprehension to reduce the bulk of the reply.
    #########################################################
    wattage = {k:v for k, v in blob.items() if k in wattage_keys}
    temps = {k:v for k,v in blob.items() if k in temperature_keys}

    # The wattage table is normalized. 
    for k, v in wattage.items():
        point = db_names[k]
        for kk, vv in v.items():
            db.execute_SQL(sql_statement, t, node_dict[kk], point, vv)
        db.commit()

    # The temperature table is not /completely/ normalized.
    temperatures_by_node = join_dicts_by_key(temps)
        
    return os.EX_OK


def dither_time(t:int) -> int:
    """
    Avoid measuring the power at regular intervals.
    """
    lower = int(t * 0.95)
    upper = int(t * 1.05)
    while True:
        yield random.randint(lower, upper)    


@trap
def join_dicts_by_key(*my_dicts) -> dict:
    """
    Make a new dict with all the keys in my_dicts,
    and all the values.
    """

    merged_dict = {}

    for k in my_dicts[0]:
        merged_dict[k] = tuple(d[k] for d in my_dicts)

    return merged_dict



@trap
def veryhungrycluster_main(myargs:argparse.Namespace) -> int:
    """
    This function just manages the loop. 
    """
    global db_handle
    global exe_statement

    # Get an explicit list of node names in case the "all" 
    # partition is undefined in this environment.
    # 
    # The [1:] slice 
    nodeinfo = dorunrun('sinfo -o "%n"', 
        return_datatype=str).strip().split('\n')[1:]
    nodenames = ",".join(nodeinfo)
    myargs.verbose and print(f"querying {nodenames}")
    exe_statement=exe_statement.format(nodenames)

    nodenumbers = tuple([ int(_[-2:]) for _ in nodeinfo ]) 
    node_dict = dict(zip(nodeinfo, nodenumbers))

    db_handle = db = SQLiteDB(myargs.db)
    myargs.verbose and print(f"Database {myargs.db} open")
    
    error=0
    n=0
    dither_iter = dither_time(myargs.freq)

    while not error and n < myargs.n:
        n += 1
        error = collect_data(db, node_dict)
        time.sleep(next(dither_iter))
    
    return error
    

if __name__=='__main__':

    ################################################################
    # Make sure we can press control-C to leave if we are running
    # interactively. Otherwise, handle it.
    ################################################################
    os.isatty(0) and caught_signals.remove(signal.SIGINT)

    parser = argparse.ArgumentParser(prog='veryhungrycluster', 
        description='keep a watch on the power')
    
    parser.add_argument('-f', '--freq', type=int, default=300,
        help='number of seconds between polls (default:300)')
    parser.add_argument('--db', type=str, default='power.db',
        help='name of database (default:"power.db")')
    parser.add_argument('-v', '--verbose', action='store_true',
        help='be chatty')
    parser.add_argument('-n', type=int, default=sys.maxsize,
        help="For debugging, limit number of readings (default:unlimited)")

    myargs = parser.parse_args()
    linuxutils.dump_cmdline(myargs)
    linuxutils.setproctitle('veryhungrycluster')

    for _ in caught_signals:
        try:
            signal.signal(_, handler)
        except OSError as e:
            myargs.verbose and sys.stderr.write(f"Cannot reassign signal {_}\n")
        else:
            myargs.verbose and sys.stderr.write(f"Signal {_} is being handled.\n")

    exit_code = ExitCode(veryhungrycluster_main(myargs))
    print(exit_code)
    sys.exit(int(exit_code))
    
