#!/usr/bin/env python2.6

import base64
import json
import os
import re
import sys
import time
import traceback
import urllib2

from socket import socket
from lib.Client import Cluster, Connection
from lib.Client import ClusterException, ConnectionException

__author__ = "Umair Ghani, Tim Ehlers, Matt Godby"


try:
    env = sys.argv[1]
except IndexError:
    print "Requires a config to be specified - %s" %configs.keys()
    sys.exit(1)


BASE_DIR = os.path.dirname(__file__)
CONF_DIR = os.path.join( BASE_DIR, "conf" )
CONF_FILE = os.path.join( CONF_DIR, "cbtest.cfg" )
CB_CONF = os.path.join( CONF_DIR, env + ".cfg" )
TIMESTAMP = int(time.time())

CBKEY="CBTEST" + time.strftime("%I:%M:%S")
#CBVALUE="Hello World"
#TTL=60


## Read the cbtest config file
with open(CONF_FILE, "r") as fd:
    testcfg = json.loads(fd.read())

## Read the couchbase config file
with open(CB_CONF, "r") as fd:
    configs = json.loads(fd.read())

def sendtographite(metrics):
    sock = socket()
    sock.connect( (configs['graphite_server'],int(configs['graphite_port'])) )
    sock.sendall(metrics)
    sock.close()

def add_key(connObj,bucket):
    time1 = time.time()
    r = connObj.add(CBKEY, testcfg['cb_value'], testcfg['cb_ttl'])
    time2 = time.time()
    print 'add_key to %s took %0.3f ms' % (bucket, (time2-time1)*1000.0)
    DATA = configs['graphite_prefix'] + '.%s.addLatency %0.3f %d\n ' % (bucket, (time2-time1)*1000.0, TIMESTAMP)
    sendtographite ( DATA )

def get_key(connObj,bucket):
    time1 = time.time()
    r = connObj.get(CBKEY)
    if r.value == testcfg['cb_value']:
        pass
    else:
        print "key missmatch %s" % r.value
    time2 = time.time()
    print 'get_key to %s took %0.3f ms' % (bucket, (time2-time1)*1000.0)
    DATA = configs['graphite_prefix'] + '.%s.getLatency %0.3f %d\n ' % (bucket, (time2-time1)*1000.0, TIMESTAMP) 
    sendtographite ( DATA )


def del_key(connObj):
    connObj.delete(CBKEY)  
 
if __name__ == "__main__":

    for cluster in configs['clusters']:
        username = configs['username']
        password = configs['password']
        bucket_url = "/pools/default/buckets"
        port = re.search('(?<=:)\w+', cluster).group(0)
        try:
            url = 'http://' + cluster + bucket_url
            request = urllib2.Request(url)
            base64string = base64.encodestring('%s:%s' % (username, password)).replace('\n', '')
            request.add_header("Authorization", "Basic %s" % base64string)
            result = urllib2.urlopen(request)  # Will get a urllib2 result object here
            data = json.load(result)  # Decode it and extract the JSON response.
            for buckets in data:
                kwargs = { 
                "vip":  cluster,
                "port": port,
                "bucket": buckets['name'],
                "username": buckets['name'],
                "password": buckets['saslPassword']
                }
                connection = Connection( **kwargs )
                try:
                    del_key( connection )
                except:
                    pass

                try:
                    add_key( connection, kwargs["bucket"] )
                    get_key( connection, kwargs["bucket"] )
                except:
                    print "I failed to connect to a bucket %s %s %s" % (kwargs["bucket"], kwargs["vip"], kwargs["port"])
                    traceback.print_exc(file=sys.stdout)
                    continue
        except:
            print "I failed to connect to cluster %s" % url
            traceback.print_exc(file=sys.stdout)
            pass
    else:
        print "Require %s" %configs.keys()
