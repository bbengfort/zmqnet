# fabfile
# Fabric command definitions for running zmq benchmarks.
#
# Author:    Benjamin Bengfort <benjamin@bengfort.com>
# Created:  Wed Jul 19 07:32:56 2017 -0400
#
# Copyright (C) 2017 Bengfort.com
# For license information, see LICENSE.txt
#
# ID: fabfile.py [] benjamin@bengfort.com $

"""
Fabric command definitions for running zmq benchmarks.
"""

##########################################################################
## Imports
##########################################################################

import os
import random

from fabric.api import env, run, cd, parallel, get
from fabric.api import roles, task, execute, settings

##########################################################################
## Environment
##########################################################################

# Names
NEVIS = "nevis.cs.umd.edu"
LAGOON = "lagoon.cs.umd.edu"
HYPERION = "hyperion.cs.umd.edu"

# Processes
PROCS = {
    NEVIS: ["nevis1", "nevis2", "nevis3"],
    LAGOON: ["lagoon20", "lagoon21", "lagoon22"],
    HYPERION: ["hyperion40", "hyperion41", "hyperion42"],
}

# Paths
workspace = "/data/alia"

# Fabric Env
env.colorize_errors = True
env.hosts = [NEVIS, LAGOON, HYPERION]
env.user = "benjamin"


def pproc_command(commands):
    """
    Creates a pproc command from a list of command strings.
    """
    commands = " ".join([
        "\"{}\"".format(command) for command in commands
    ])
    return "pproc {}".format(commands)


def round_robin(n, host, hosts=len(env.hosts)):
    """
    Returns a number n (of clients) for the specified host, by allocating the
    n clients evenly in a round robin fashion. For example, if hosts = 3 and 
    n = 5; then this function returns 2 for host[0], 2 for host[1] and 1 for
    host[2].
    """




##########################################################################
## Honu Commands
##########################################################################

@parallel
def bench(clients=1,peers="peers.json"):
    """
    Run all servers on the host as well as the specified # of benchmarks.
    """
    clients = int(clients)
    peers = os.path.join(workspace, peers)

    command = "pproc "
    for proc in PROCS[env.host]:
        command += "\"zmqnet serve -n {}\" ".format(proc, peers)

    leader = PROCS[env.host][0]
    for _ in range(clients):
        command += "\"zmqnet bench -n {} -c {}\" ".format(leader, clients)

    with cd(workspace):
        run(command)

@parallel
def getmerge(localpath="results"):
    local = os.path.join(localpath, "%(host)s", "%(path)s")
    remote = os.path.join("/data/alia", "results.json")
    get(remote, local)
