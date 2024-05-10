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
from   parsec4 import *
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

caught_signals = [  
    signal.SIGINT, 
    signal.SIGQUIT,
    signal.SIGUSR1, 
    signal.SIGUSR2, 
    signal.SIGTERM 
    ]

# This is the ACT program that gathers data from nodes.
exe=shutil.which('cv-stats')
# Request the info in JSON.
exe_statement=f"{exe} --nodes {{}} --format json"

# There is only one thing we do; insert a few rows.
# CREATE TABLE facts (time int, node varchar(10), point varchar(20), temperature int);
sql_statement="""
    INSERT INTO FACTS (t, node, point, temp) VALUES (?, ?, ?, ?)
    """



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


def collect_temperatures(db:object, node_dict:dict) -> int:
    """
    Use ACT's tools to poll the nodes and write the fact table
    of the database.
    """
    global exe_statement
    blob=json.loads(dorunrun(
        exe_statement, return_datatype=str).strip())

    #########################################################
    # The times for reading each node are not significantly
    # different from each other, so take the first one.
    #########################################################
    t = int(time.time())

    for k, v in node_dict:
        blob = json.loads(dorunrun(exe_statement(v), return_datatype=str).strip())

        #########################################################
        # Use a dict comprehension to reduce the bulk of the reply.
        #########################################################
        temps = { transform(k):explode(v) for k, v in blob.items() if "temperature_value" in k }
        for point, value in temps.items()
            db.execute_SQL(sql_statement(t, point, v, value[1]))
        
    return os.EX_OK


def dither_time(t:int) -> int:
    """
    Avoid measuring the power at regular intervals.
    """
    lower = int(t * 0.95)
    upper = int(t * 1.05)
    while True:
        yield random.randint(lower, upper)    


def explode(d:dict) -> tuple:
    """
    only needs to do one.
    """
    for k, v in d.items(): 
        return k, v


def transform(s:str) -> str:
    """
    Rename the wordy keys that come back from cv-stat.
    """
    _, s = s.split(EQ, 1)
    s, _ = s.split(COMMA, 1)

    return s 


@trap
def heat_main(myargs:argparse.Namespace) -> int:
    """
    This function just manages the loop. 
    """
    global db_handle
    global exe_statement

    # Get an explicit list of node names in case the "all" 
    # partition is undefined in this environment.
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
        error = collect_temperatures(db, node_dict)
        time.sleep(next(dither_iter))
    
    return error
    

if __name__=='__main__':

    ################################################################
    # Make sure we can press control-C to leave if we are running
    # interactively. Otherwise, handle it.
    ################################################################
    os.isatty(0) and caught_signals.remove(signal.SIGINT)

    parser = argparse.ArgumentParser(prog='heat', 
        description='keep a watch on the temperatures')
    
    parser.add_argument('-f', '--freq', type=int, default=300,
        help='number of seconds between polls (default:300)')
    parser.add_argument('--db', type=str, default='temps.db',
        help='name of database (default:"temps.db")')
    parser.add_argument('-v', '--verbose', action='store_true',
        help='be chatty')
    parser.add_argument('-n', type=int, default=sys.maxsize,
        help="For debugging, limit number of readings (default:unlimited)")

    myargs = parser.parse_args()
    linuxutils.dump_cmdline(myargs)
    linuxutils.setproctitle('heat')

    for _ in caught_signals:
        try:
            signal.signal(_, handler)
        except OSError as e:
            myargs.verbose and sys.stderr.write(f"Cannot reassign signal {_}\n")
        else:
            myargs.verbose and sys.stderr.write(f"Signal {_} is being handled.\n")

    exit_code = ExitCode(heat_main(myargs))
    print(exit_code)
    sys.exit(int(exit_code))
    
