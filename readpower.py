# -*- coding: utf-8 -*-

import os
import sys

# Standard imports.

import argparse
import collections
import json
import pandas
try:
    import pyarrow
    no_pyarrow = False
except ImportError as e:
    sys.write("The feather dataformat is unavailable")
    no_pyarrow = True
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


def pivot(myargs:argparse.Namespace, frame:pandas.DataFrame) -> pandas.DataFrame:
    """
    Translate the fact table.
    """
    column_data = collections.defaultdict(pandas.Series)
    for node_number in myargs.node:
        new_frame = frame[frame['node']==int(node_number)]
        column_data[node_number] = pandas.Series(new_frame['watts'].values, index=new_frame['t'], name=str(node_number))

    new_frame = pandas.DataFrame(column_data[myargs.node[0]])
    for node_number in myargs.node[1:]:
        new_frame = new_frame.join(column_data[node_number])

    return new_frame
        


def readpower_main(myargs:argparse.Namespace) -> int:

    earliest = 0 if not myargs.time else time.time() - myargs.time*24*60*60
    
    SQL = f"select * from facts where t > {earliest} " 
    if myargs.node: SQL += f" and node in ({','.join(myargs.node)}) "
    if myargs.point: SQL += f" and point = '{myargs.point}' "
    SQL += " order by t asc"
    myargs.verbose and print(SQL)

    db=SQLiteDB(myargs.db)
    frame=pandas.read_sql(SQL, db.db)
    frame['t'] = pandas.to_datetime(frame['t'], unit='s')


    
    if myargs.point and myargs.pivot and len(myargs.node) > 1:
        frame = pivot(myargs, frame)
    else:
        frame['point'] = frame['point'].str.replace('c', 'cpu')
        frame['point'] = frame['point'].str.replace('m', 'mem')
        frame['point'] = frame['point'].str.replace('t', 'total')
    
    frame.index.name = 'time_utc'
    getattr(frame, f"to_{myargs.format}")(f"{myargs.output}.{myargs.format}")

    return os.EX_OK


if __name__=='__main__':

    parser = argparse.ArgumentParser(prog='readpower', 
        description='analyze the power data we have written.')
    
    parser.add_argument('--db', type=str, default='power.db',
        help='name of database (default:"power.db")')

    if no_pyarrow:
        formats=("csv", "pandas", "stata", "parquet")
    else:
        formats=("csv", "feather", "pandas", "stata", "parquet")

    parser.add_argument('--pivot', action='store_true', 
        help='translate fact table into the usual tabular format')

    parser.add_argument('--format', type=str, default="csv", choices=formats,
        help="Output format; default is csv")

    parser.add_argument('-n', '--node', default=[ str(_) for _ in list(range(1,19))+list(range(50,62)) ],
        action='append',
        help='node number to investigate (default is all)')

    parser.add_argument('-o', '--output', type=str, default="facts",
        help='''name of output file for extracted data. A suffix will 
be added to reflect the data format.''')

    parser.add_argument('-p', '--point', type=str, default="",
        choices=('c', 'm', 't'),
        help='measurement point to consider (default is all)')

    parser.add_argument('-t', '--time', type=int, default=1,
        help='number of recent 24-hour periods to consider (default=1)')

    parser.add_argument('-v', '--verbose', action='store_true',
        help='be chatty')

    myargs = parser.parse_args()
    myargs.verbose and linuxutils.dump_cmdline(myargs)
    if myargs.format == 'pandas': myargs.format='pickle'

    sys.exit(readpower_main(myargs))
