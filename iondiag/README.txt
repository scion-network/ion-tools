===============================================================================
Tools to work with OOINet (ION) system
===============================================================================

iondiag.py
==========

INSTALLATION
------------

Requires a tools virtualenv:
> mkvirtualenv --no-site-packages tools

Install dependencies:
> pip install -r requirements.txt

CONFIGURATION
-------------

Put a .cfg (YML syntax) file somewhere by copying the template iondiag_example.cfg to iondiag.cfg
Set sysname plus rabbit/postgres connection info.
The default config file is ./iondiag.cfg. The -c option allows to set another path.

USAGE
-----

Start tool with another config file
> python iondiag.py -c mycfg.cfg

Retrieve system info and store/overwrite in ./sysinfo dir
> python iondiag.py -d sysinfo

Run diagnosis based on content in ./sysinfo dir
> python iondiag.py -d sysinfo -l

Retrieve rabbit info only and get rest from content in ./sysinfo dir if existing
> python iondiag.py -d sysinfo -R r

Verbose output
> python iondiag.py -v

Interactive shell
> python iondiag.py -d sysinfo -l -i

INTERACTIVE MODE
----------------

The shell exposes self variables and functions after the analysis.

Try dir(self)

Members:
  self._zoo - CEI Zookeeper directory
  self._procs_by_type

Functions:
  ts - coverts a system timestamp into a string
  self._get_zoo_connection
  self._get_db_connection

Expressions:
  List EPUIs with last state time and state
    sorted(["%s/%s: %s %s" % (x["epu"],x["name"], ts(self._zoo[x["zoo"]]["state_time"]), x["state"]) for x in self._epuis.values()])

  Active HA Agents:
    [self._procs[x] for x in self._proc_by_type["ha_agent"] if x in self._procs]

  Queues with messages:
    ["%s:%s" % (q["name"], q["messages"]) for q in self._named_queues.values() if q["messages"]>2]