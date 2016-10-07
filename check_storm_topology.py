#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: set fileencoding=utf-8
#
# Copyright (C) 2016 rahulinux - All Rights Reserved
# Permission to copy and modify is granted under the MIT license
#
#    http://www.linuxian.com/
#
# or in the "license" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.

# Name	         : check_storm_topology.py 
# Purpose        : check error in topology 
# Date           : Thu Oct  6 08:06:51 UTC 2016
# Python Version : 2.6.6
# Author         : Rahul Patil
# Version        : 0.1 

import os
import sys
import time
import json
import socket
import docopt
import urllib2
import datetime
import subprocess

usage = """
Usage: 
    check_storm_topology.py  --ip <nimbus_server_ip> [--port=p] [--topology=topology ...]

Options:
  --ip                     specify ip address of nimbus.
  --port=8080              [default: 8080]. 
  --topology=topologyname  topology name [default: all]

Examples:
    check_storm_topology.py --ip 10.22.10.150 
    check_storm_topology.py --ip 10.22.10.150 --port 8080 
    check_storm_topology.py --ip 10.22.10.150 --port 8080 --topology mytopology 
    check_storm_topology.py --ip 10.22.10.150 --topology mytopology1 --topology mytopology2 
"""


error_found = False
status_ok = 0
status_warn = 1
status_critical = 2


def basic_checks(server,port):
    """Perform basics checks on given host"""
    sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    # 2 seconds timeout
    sock.settimeout(2)  
    return sock.connect_ex((server,int(port))) == 0
       

def get_available_topologies(server,topology="all"):
    """Return topology ids
    will be use to track error in given topology id
    if topology not provided it will check all 
    available topologies, but we recommend to 
    specify topology because sometime topology 
    is not deployed or killed
    """
    nimbus_url = "http://{0}:{1}/api/v1/topology/summary".format(server,port)
    nimbus_data = urllib2.urlopen(nimbus_url)
    ids = json.loads(nimbus_data.read())
    if topology == "all":
       tid = [ d['id'] for d in ids['topologies'] ]
    else:
       tid = [ d['id'] for d in ids['topologies'] if str(topology) in str(d['id']) ]
    return tid
      

def check_error(topology_id):
     """Check error in spouts 
     and botls and print errors
     return false if any error found
     """ 
     global error_found
     nimbus_url = "http://{0}:{1}/api/v1/topology/{2}".format(server,port,topology_id)
     data = json.loads(urllib2.urlopen(nimbus_url).read()) 
     print "Checking error in {0} topology".format(topology_id) 
     for spouts in data['spouts']:
        if spouts['lastError']:
             print spouts['lastError']
             print spouts['spoutId']
             error_found = True
     for bolts in data['bolts']:
        if bolts['lastError']:
             print bolts['lastError']
             print bolts['boltId']
             error_found = True
     return error_found


def check_topology(topology="all"):
    """get topology id using topology name
    and check error in topologies
    """
    global error_found
    if topology == "all":
       for topology_id in get_available_topologies(server):
           error_found=check_error(topology_id)
    else:
           if type(topology) == list:
              for t in topology:
                  print t
                  topology_id = ''.join(get_available_topologies(server,t))
                  if topology_id == "":
                    print "{0} topology not found".format(t)
                    error_found=True
           else:
              topology_id = ''.join(get_available_topologies(server,topology))
              if topology_id == "":
                 print "{0} topology not found".format(topology)
                 error_found=check_error(topology_id)
   
    return error_found               


if __name__ == '__main__':
    args = docopt.docopt(usage,version='version 0.1 by Rahul Patil<http://linuxian.com>',options_first=False)
    server,port,topology = args['<nimbus_server_ip>'],args['--port'],args['--topology']
    if  basic_checks(server,port) == False:
       print "Unable to connect remote host <{0}:{1}>".format(server,port)
       sys.exit(status_critical)
    if check_topology(topology):
       sys.exit(status_critical)
    else:
       print "No Error Found in topology"
       sys.exit(status_ok)
