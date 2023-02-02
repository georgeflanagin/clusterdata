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
    formats=("csv", "feather", "pandas", "stata", "parquet")
except ImportError as e:
    sys.write("The feather dataformat is unavailable")
    no_pyarrow = True
    formats=("csv", "pandas", "stata", "parquet")

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
__copyright__ = 'Copyright 2021, University of Richmond'
__credits__ = None
__version__ = 1.0
__maintainer__ = 'George Flanagin'
__email__ = ['me@georgeflanagin.com', 'gflanagin@richmond.edu']
__status__ = 'Early production'
__license__ = 'MIT'


####
# Globals
####
# Make all_nodes a tuple so that it is immutable. Create a zip/dict
# of all the background power consumption.
####
all_nodes = tuple([ str(_) for _ in list(range(1,19))+list(range(50,62)) ])
idle_power = tuple([115, 115, 115, 115, 115, 115, 115, 115,  # basic
    120, 120, 120, 120, 120, # medium
    135, 135, # large
    425, # ML
    850, 850, # sci
    115, # bukach
    135, 135, # dias
    120, # erickson
    115, # johnson
    120, 120, 120, 120, # parish
    135, # yang1
    115, # yang2
    115]) # yangnolin
max_power = tuple([650, 650, 650, 650, 650, 650, 650, 650,  # basic
    700, 700, 700, 700, 700, # medium
    800, 800, # large
    1000, # ML
    2000, 2000, # sci
    650, # bukach
    800, 800, # dias
    700, # erickson
    650, # johnson
    700, 700, 700, 700, # parish
    800, # yang1
    650, # yang2
    650]) # yangnolin
tares = dict(zip(all_nodes, idle_power))    
limits = dict(zip(all_nodes, max_power))
cluster_max = sum(max_power)
cluster_tare = sum(idle_power)

def pivot(myargs:argparse.Namespace, frame:pandas.DataFrame) -> pandas.DataFrame:
    """
    Translate the fact table, and apply tare if requested.
    """
    global tares

    column_data = collections.defaultdict(pandas.Series)
    for node_number in myargs.node:
        myargs.verbose and print(f"filtering node {node_number}")
        new_frame = frame[frame['node']==int(node_number)]
        column_data[node_number] = pandas.Series(new_frame['watts'].values, 
            index=new_frame['t'], 
            name=str(node_number))
        if myargs.tare: 
            column_data[node_number] = column_data[node_number].subtract(
                                        tares[node_number], fill_value=0)

    myargs.verbose and print("Building pivot table with concat")
    new_frame = pandas.concat([ c for c in column_data.values() ], axis=1)
    new_frame['cluster'] = new_frame.sum(axis=1)
    if myargs.tare: new_frame[new_frame < 0] = 0

    myargs.verbose and print("Pivot complete.")
    return new_frame
        

def readpower_main(myargs:argparse.Namespace) -> int:
    """
    Because of the need to dynamically build the SQL used
    to retrieve the pandas data, this function is a little
    klutzy compared with most of the _main functions in 
    command line utilities.
    """

    # time == 0 means all the records in the database.
    earliest = 0 if not myargs.time else time.time() - myargs.time*24*60*60
    
    # The messy construction of the SQL.
    SQL = f"select * from facts where t > {earliest} " 
    if myargs.node != all_nodes: SQL += f" and node in ({','.join(myargs.node)}) "
    if myargs.point: SQL += f" and point = '{myargs.point}' "
    SQL += " order by t asc, node asc"
    myargs.verbose and print(SQL)

    # Open the database.
    db=SQLiteDB(myargs.db)
    myargs.verbose and print("Database opened.")

    # Get the data.
    frame=pandas.read_sql(SQL, db.db)
    myargs.verbose and print(f"Data read. {frame.shape=}")
    frame['t'] = pandas.to_datetime(frame['t'], unit='s')
    myargs.verbose and print("ISO seconds converted to timestamp")

    # See if we need to save the raw pandas data.
    if myargs.save_frame:
        frame_file = 'rawframe.csv'
        os.path.exists(frame_file) and os.unlink(frame_file)
        frame.to_csv(frame_file, index=False)

    # There are two different operations here. One is to pivot
    # and summarize, and the other is to retain the fact table.
    if myargs.point and myargs.pivot and len(myargs.node) > 1:
        myargs.verbose and print("Calling pivot")
        frame = pivot(myargs, frame)
        myargs.verbose and print("Pivoted.")
    else:
        frame['point'] = frame['point'].str.replace('c', 'cpu')
        frame['point'] = frame['point'].str.replace('m', 'mem')
        frame['point'] = frame['point'].str.replace('t', 'total')
    
    # Set the index. 
    frame.index.name = 'time_utc'
    myargs.verbose and print("re-indexed")
    if myargs.summarize:
        summary_frame = pandas.concat([frame['cluster']], axis=1)
        frame = summary_frame
        if myargs.percent:
            frame = frame.div(cluster_max/200)
            

    # Based on what is desired as output, call the appropriate 
    # function. 
    getattr(frame, f"to_{myargs.format}")(f"{myargs.output}.{myargs.format}")
    myargs.verbose and print(f"output written to {myargs.output}.{myargs.format}")
    return os.EX_OK


if __name__=='__main__':

    parser = argparse.ArgumentParser(prog='readpower', 
        description='analyze the power data we have written.')
    
    parser.add_argument('--db', type=str, default='power.db',
        help='name of database (default:"power.db")')

    parser.add_argument('--format', type=str, default="csv", choices=formats,
        help="Output format; default is csv")

    parser.add_argument('-n', '--node', default=all_nodes,
        action='append',
        help='node number to investigate (default is all)')

    parser.add_argument('-o', '--output', type=str, default="facts",
        help='''name of output file for extracted data. A suffix will 
be added to reflect the data format.''')

    parser.add_argument('-p', '--point', type=str, default="t",
        choices=('c', 'm', 't'),
        help='measurement point to consider (default is "t", for total)')

    parser.add_argument('--percent', action='store_true',
        help='fractional measure of usage. Note: *can* exceed 100%')

    parser.add_argument('--pivot', action='store_true', 
        help='translate fact table into the usual tabular format')

    parser.add_argument('-s', '--summarize', action='store_true',
        help='cluster total power against time, ONLY')

    parser.add_argument('--save-frame', action='store_true',
        help='save the pandas DataFrame to disc for later use.')

    parser.add_argument('-t', '--time', type=int, default=1,
        help='number of recent 24-hour periods to consider (default=1)')

    parser.add_argument('--tare', action='store_true', 
        help="""--tare removes the idle power from the readings of the node-total.
This setting only makes sense if the total is being read.""")
    
    parser.add_argument('--totals', action='store_true',
        help="equivalent to all nodes and --point t")

    parser.add_argument('-v', '--verbose', action='store_true',
        help='be chatty')

    myargs = parser.parse_args()

    # A little semantic checking is required.
    if myargs.format == 'pandas': myargs.format='pickle'
    if myargs.totals: 
        myargs.node = all_nodes
        myargs.point = 't'
    if myargs.tare and 't' not in myargs.point:
        print("--tare cannot be used unless the total is being read.")
        parser.print_help()

    myargs.verbose and linuxutils.dump_cmdline(myargs)

    sys.exit(readpower_main(myargs))
