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
    
    db=SQLiteDB(myargs.db)
    frame=pandas.read_sql("select * from facts", db.db)
    print(frame)

    return os.EX_OK


if __name__=='__main__':

    parser = argparse.ArgumentParser(prog='readpower', 
        description='analyze the power data we have written.')
    
    parser.add_argument('--db', type=str, default='power.db',
        help='name of database (default:"power.db")')
    parser.add_argument('-v', '--verbose', action='store_true',
        help='be chatty')

    myargs = parser.parse_args()
    linuxutils.dump_cmdline(myargs)

    sys.exit(readpower_main(myargs))
