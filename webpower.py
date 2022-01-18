# -*- coding: utf-8 -*-
import typing
from   typing import *

min_py = (3, 8)

###
# Standard imports, starting with os and sys
###
import os
import sys
if sys.version_info < min_py:
    print(f"This program requires Python {min_py[0]}.{min_py[1]}, or higher.")
    sys.exit(os.EX_SOFTWARE)

###
# Other standard distro imports
###
import argparse
import random
import shutil
import signal
import time

###
# From hpclib
###
from   dorunrun import dorunrun
import linuxutils
from   sloppytree import SloppyTree
from   sqlitedb import SQLiteDB
from   urdecorators import trap

###
# Credits
###
__author__ = 'George Flanagin'
__copyright__ = 'Copyright 2022'
__credits__ = None
__version__ = 0.1
__maintainer__ = 'George Flanagin'
__email__ = ['gflanagin@richmond.edu', 'me@georgeflanagin.com']
__status__ = 'in progress'
__license__ = 'MIT'

caught_signals = [  signal.SIGINT, signal.SIGQUIT,
                    signal.SIGUSR1, signal.SIGUSR2, signal.SIGTERM ]
db_handle = None
exe = shutil.which('w')
sql_statement="""INSERT INTO W_FACTS (one_minute, five_minutes, fifteen_minutes) 
    VALUES (?, ?, ?)"""


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


def collect_usage(db) -> int:

    global sql_statement
    global exe

    results = SloppyTree(dorunrun(exe, return_datatype=dict))
    if not results.OK: return os.EX_DATAERR

    data = tuple(results.stdout.split('\n')[0].replace(',',' ').split()[-3:])
    db.execute_SQL(sql_statement, *data)
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
def webpower_main(myargs:argparse.Namespace) -> int:

    db_handle = db = SQLiteDB(myargs.db)

    error=os.EX_OK
    n=0
    dither_iter = dither_time(myargs.freq)

    while not error and n < myargs.n:
        n += 1
        error = collect_usage(db)
        time.sleep(next(dither_iter))
    
    return error


if __name__ == '__main__':
    
    os.isatty(0) and caught_signals.remove(signal.SIGINT)

    parser = argparse.ArgumentParser(prog="webpower", 
        description="What webpower does, webpower does best.")

    parser.add_argument('-f', '--freq', type=int, default=300,
        help='number of seconds between polls (default:300)')
    parser.add_argument('--db', type=str, default='/usr/local/sw/perfdata/webpower.db',
        help='name of database (default:"power.db")')
    parser.add_argument('-n', type=int, default=sys.maxsize,
        help="For debugging, limit number of readings (default:unlimited)")

    myargs = parser.parse_args()

    for _ in caught_signals:
        try:
            signal.signal(_, handler)
        except OSError as e:
            sys.stderr.write(f"Cannot reassign signal {_}\n")
        else:
            sys.stderr.write(f"Signal {_} is being handled.\n")

    try:
        sys.exit(
            globals()["{}_main".format(os.path.basename(__file__)[:-3])](myargs)
            )

    except Exception as e:
        print(f"Escaped or re-raised exception: {e}")


