# -*- coding: utf-8 -*-

import os
import sys

# Standard imports.

import argparse
import json
import pandas
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


def readpower_main(myargs:argparse.Namespace) -> int:

    earliest = 0 if not myargs.time else time.time() - myargs.time*24*60*60
    
    SQL = f"select * from facts where t > {earliest} " 
    if myargs.node: SQL += f" and node in ({','.join(myargs.node)}) "
    if myargs.point: SQL += f" and point = '{myargs.point}' "
    SQL += " order by t asc"
    myargs.verbose and print(SQL)

    db=SQLiteDB(myargs.db)
    frame=pandas.read_sql(SQL, db.db)
    if myargs.format == 'csv':
        frame['t'] = pandas.to_datetime(frame['t'], unit='s')
        frame.to_csv(myargs.output, index=False)
    else:
        frame.to_pickle(myargs.output)

    return os.EX_OK


if __name__=='__main__':

    parser = argparse.ArgumentParser(prog='readpower', 
        description='analyze the power data we have written.')
    
    parser.add_argument('--db', type=str, default='power.db',
        help='name of database (default:"power.db")')

    parser.add_argument('--format', type=str, default="csv",
        choices=("csv", "pandas"),
        help="csv or pandas output.")

    parser.add_argument('-n', '--node', default=[],
        action='append',
        help='node number to investigate (default is all)')

    parser.add_argument('-o', '--output', type=str, default="facts.csv",
        help='name of output file for extracted data.')

    parser.add_argument('-p', '--point', type=str, default="",
        choices=('c', 'm', 't'),
        help='measurement point to consider (default is all)')

    parser.add_argument('-t', '--time', type=int, default=1,
        help='number of recent 24-hour periods to consider (default=1)')

    parser.add_argument('-v', '--verbose', action='store_true',
        help='be chatty')

    myargs = parser.parse_args()
    myargs.verbose and linuxutils.dump_cmdline(myargs)

    sys.exit(readpower_main(myargs))
