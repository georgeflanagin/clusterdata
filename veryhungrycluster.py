
import os
import sys

import argparse
import json
import shutil
import signal
import time

from   dorunrun import dorunrun, ExitCode
import linuxutils
from   sqlitedb import SQLiteDB
from   urdecorators import show_exceptions_and_frames as trap

#################################################
# This program has a lot of static data
#################################################

caught_signals = [  signal.SIGINT, signal.SIGQUIT,
                    signal.SIGUSR1, signal.SIGUSR2, signal.SIGTERM ]
exe=shutil.which('cv-stats')
exe_statement=f"{exe} --nodes {{}} --format json"
sql_statement="""INSERT INTO FACTS (t, node, point, watts) 
    VALUES (?, ?, ?, ?)"""
wattage_keys = (
    'power.cpu_watts', 'power.memory_watts', 'power.node_watts',
    )
db_keys = ('c', 'm', 't', )
db_names = dict(zip(wattage_keys, db_keys))

##################################################
# But there is only one global, and it is needed
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
            sys.stderr.write(f"Error on exit {e}")
            sys.exit(os.EX_IOERR)
        else:
            sys.exit(os.EX_OK)
    else:
        return


def collect_power_data(db:object, nodenames:str, node_dict:dict) -> int:
    """
    Use ACT's tools to poll the nodes and write the fact table
    of the database.
    """
    stmt=exe_statement.format(nodenames)
    blob=json.loads(dorunrun(stmt, return_datatype=str).strip())

    #########################################################
    # The times for reading each node are not significantly
    # different from each other, so take the first one.
    #########################################################
    t = int(list(blob['_timestamp'].values())[0])

    #########################################################
    # Use a dict comprehension to reduce the bulk of the reply.
    #########################################################
    wattage = {k:v for k, v in blob.items() if k in wattage_keys}
    for k, v in wattage.items():
        point = db_names[k]
        for kk, vv in v.items():
            db.execute_SQL(sql_statement, t, node_dict[kk], point, vv)
        db.commit()
        
    return os.EX_OK


@trap
def veryhungrycluster_main(myargs:argparse.Namespace) -> int:
    """
    This function just manages the loop. 
    """
    global db_handle

    # Get an explicit list of node names in case the "all" 
    # partition is undefined in this environment.
    nodeinfo = dorunrun('sinfo -o "%n"', 
        return_datatype=str).strip().split('\n')[1:]
    nodenames = ",".join(nodeinfo)
    nodenumbers = tuple([ int(_[-2:]) for _ in nodeinfo ]) 
    node_dict = dict(zip(nodeinfo, nodenumbers))

    myargs.verbose and print(f"querying {nodenames}")
    db_handle = db = SQLiteDB(myargs.db)
    myargs.verbose and print(f"Database {myargs.db} open")
    
    error=0
    n=0
    while not error and n < myargs.n:
        n += 1
        error = collect_power_data(db, nodenames, node_dict)
        time.sleep(myargs.freq)
    
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
    
