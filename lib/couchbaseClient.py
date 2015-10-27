#
# Copyright 2011, Couchbase, Inc.
# All Rights Reserved
#
# Licensed under the Apache License, Version 2.0 (the "License")
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import sys
import base64
import json
import urllib
import httplib2
import socket
import time
#import logger
from exception import ServerAlreadyJoinedException, ServerUnavailableException, InvalidArgumentException
from exception import BucketCreationException, ServerJoinException

#log = logger.logger("membase_client.log")

class RestConnection(object):

    def __init__(self, serverInfo):
        #serverInfo can be a json object
        if isinstance(serverInfo, dict):
 #           self.log = logger
            self.ip = serverInfo["ip"]
            self.username = serverInfo["username"]
            self.password = serverInfo["password"]
            self.port = serverInfo["port"]
            self.baseUrl = "http://{0}:{1}/".format(self.ip, self.port)
        else:
            sys.exit(1)

    #authorization mut be a base64 string of username:password
    def _create_headers(self):
        authorization = base64.encodestring('%s:%s' % (self.username, self.password))
        return {'Content-Type': 'application/x-www-form-urlencoded',
                'Authorization': 'Basic %s' % authorization,
                'Accept': '*/*'}

    def log_client_error(self, post):
        api = self.baseUrl + 'logClientError'
        try:
            response, content = httplib2.Http().request(api, 'POST', body=post, headers=self._create_headers())
            #if status is 200 then it was a success otherwise it was a failure
            if response['status'] == '400':
                print 'unable to logClientError'
                #extract the error
                #self.log.error('unable to logClientError')
        except socket.error:
            raise ServerUnavailableException(ip=self.ip)
        except httplib2.ServerNotFoundError:
            raise ServerUnavailableException(ip=self.ip)

    #retuns node data for this host
    def get_nodes_self(self):
        node = None
        api = self.baseUrl + 'nodes/self'
        try:
            response, content = httplib2.Http().request(api, headers=self._create_headers())
            #if status is 200 then it was a success otherwise it was a failure
            if response['status'] == '400':
                #extract the error
                #self.log.error('unable to retrieve nodesStatuses')
                print 'unable to retrieve nodesStatuses'
            elif response['status'] == '200':
                parsed = json.loads(content)
                node = RestParser().parse_get_nodes_response(parsed)
        except socket.error:
            raise ServerUnavailableException(ip=self.ip)
        except httplib2.ServerNotFoundError:
            raise ServerUnavailableException(ip=self.ip)
        return node

    def node_statuses(self):
        nodes = []
        api = self.baseUrl + 'nodeStatuses'
        try:
            response, content = httplib2.Http().request(api, headers=self._create_headers())
            #if status is 200 then it was a success otherwise it was a failure
            if response['status'] == '400':
                #extract the error
                #self.log.error('unable to retrieve nodesStatuses')
                print 'unable to retrieve nodesStatuses'
            elif response['status'] == '200':
                parsed = json.loads(content)
                for key in parsed:
                    #each key contain node info
                    value = parsed[key]
                    #get otp,get status
                    node = OtpNode(id=value['otpNode'],
                                   status=value['status'])
                    if node.ip == '127.0.0.1':
                        node.ip = self.ip
                    node.port = int(key[key.rfind(":") + 1:])
                    node.replication = value['replication']
                    nodes.append(node)
                    #let's also populate the membase_version_info
            return nodes
        except socket.error:
            raise ServerUnavailableException(ip=self.ip)
        except httplib2.ServerNotFoundError:
            raise ServerUnavailableException(ip=self.ip)

    def cluster_status(self):
        parsed = []
        api = self.baseUrl + 'pools/default'
        try:
            response, content = httplib2.Http().request(api, headers=self._create_headers())
            #if status is 200 then it was a success otherwise it was a failure
            if response['status'] == '400':
                #extract the error
                #self.log.error('unable to retrieve pools/default')
                print 'unable to retrieve pools/default'
            elif response['status'] == '200':
                parsed = json.loads(content)
            return parsed
        except socket.error:
            raise ServerUnavailableException(ip=self.ip)
        except httplib2.ServerNotFoundError:
            raise ServerUnavailableException(ip=self.ip)

    def get_pools_info(self):
        api = self.baseUrl + 'pools'
        try:
            response, content = httplib2.Http().request(api, headers=self._create_headers())
            if response['status'] == '400':
                #self.log.error('get_pools error {0}'.format(content))
                print 'get_pools error {0}'.format(content)
            elif response['status'] == '200':
                parsed = json.loads(content)
                return parsed
        except socket.error:
            raise ServerUnavailableException(ip=self.ip)
        except httplib2.ServerNotFoundError:
            raise ServerUnavailableException(ip=self.ip)

    def get_pools(self):
        version = None
        api = self.baseUrl + 'pools'
        try:
            response, content = httplib2.Http().request(api, headers=self._create_headers())
            if response['status'] == '400':
                #self.log.error('get_pools error {0}'.format(content))
                print 'get_pools error {0}'.format(content)
            elif response['status'] == '200':
                parsed = json.loads(content)
                version = MembaseServerVersion(parsed['implementationVersion'], parsed['componentsVersion'])
            return version
        except socket.error:
            raise ServerUnavailableException(ip=self.ip)
        except httplib2.ServerNotFoundError:
            raise ServerUnavailableException(ip=self.ip)

    def get_buckets(self):
        #get all the buckets
        buckets = []
        api = '{0}{1}'.format(self.baseUrl, 'pools/default/buckets/')
        try:
            response, content = httplib2.Http().request(api, headers=self._create_headers())
            if response['status'] == '400':
                #self.log.error('get_buckets error {0}'.format(content))
                print 'get_buckets error {0}'.format(content)
            elif response['status'] == '200':
                parsed = json.loads(content)
                # for each elements
                for item in parsed:
                    bucketInfo = RestParser().parse_get_bucket_json(item)
                    buckets.append(bucketInfo)
                return buckets
        except socket.error:
            raise ServerUnavailableException(ip=self.ip)
        except httplib2.ServerNotFoundError:
            raise ServerUnavailableException(ip=self.ip)
        return buckets

    def get_bucket_stats_for_node(self, bucket='default', node_ip=None):
        if not Node:
            #self.log.error('node_ip not specified')
            print 'node_ip not specified'
            return None
        api = "{0}{1}{2}{3}{4}{5}".format(self.baseUrl, 'pools/default/buckets/',
                                          bucket, "/nodes/", node_ip, ":8091/stats")
        try:
            response, content = httplib2.Http().request(api, headers=self._create_headers())
            if response['status'] == '400':
                #self.log.error('get_bucket error {0}'.format(content))
                print 'get_bucket error {0}'.format(content)
            elif response['status'] == '200':
                parsed = json.loads(content)
                #let's just return the samples
                #we can also go through the list
                #and for each item remove all items except the first one ?
                op = parsed["op"]
                samples = op["samples"]
                stats = {}
                #for each sample
                for stat_name in samples:
                    stats[stat_name] = samples[stat_name][0]
                return stats
        except socket.error:
            raise ServerUnavailableException(ip=self.ip)
        except httplib2.ServerNotFoundError:
            raise ServerUnavailableException(ip=self.ip)

    def get_nodes(self):
        nodes = []
        api = self.baseUrl + 'pools/default'
        try:
            response, content = httplib2.Http().request(api, headers=self._create_headers())
            #if status is 200 then it was a success otherwise it was a failure
            if response['status'] == '400':
                #extract the error
                #self.log.error('unable to retrieve nodesStatuses')
                print 'unable to retrieve nodesStatuses'
            elif response['status'] == '200':
                parsed = json.loads(content)
                if "nodes" in parsed:
                    for json_node in parsed["nodes"]:
                        node = RestParser().parse_get_nodes_response(json_node)
                        node.rest_username = self.username
                        node.rest_password = self.password
                        node.port = self.port
                        if node.ip == "127.0.0.1":
                            node.ip = self.ip
                        nodes.append(node)
        except socket.error:
            raise ServerUnavailableException(ip=self.ip)
        except httplib2.ServerNotFoundError:
            raise ServerUnavailableException(ip=self.ip)
        return nodes

    def get_bucket_stats(self, bucket='default'):
        api = "{0}{1}{2}{3}".format(self.baseUrl, 'pools/default/buckets/', bucket, "/stats")
        try:
            response, content = httplib2.Http().request(api, headers=self._create_headers())
            if response['status'] == '400':
                #self.log.error('get_bucket error {0}'.format(content))
                print 'get_bucket error {0}'.format(content)
            elif response['status'] == '200':
                parsed = json.loads(content)
                #let's just return the samples
                #we can also go through the list
                #and for each item remove all items except the first one ?
                op = parsed["op"]
                samples = op["samples"]
                stats = {}
                #for each sample
                for stat_name in samples:
                    if samples[stat_name]:
                        last_sample = len(samples[stat_name]) - 1
                        if last_sample:
                            stats[stat_name] = samples[stat_name][last_sample]
                return stats
        except socket.error:
            raise ServerUnavailableException(ip=self.ip)
        except httplib2.ServerNotFoundError:
            raise ServerUnavailableException(ip=self.ip)

    def get_bucket(self, bucket='default'):
        bucketInfo = None
        api = '{0}{1}{2}'.format(self.baseUrl, 'pools/default/buckets/', bucket)
        try:
            if self.password:
                response, content = httplib2.Http().request(api, headers=self._create_headers())
            else:
                response, content = httplib2.Http().request(api)
            if response['status'] == '400':
                #self.log.error('get_bucket error {0}'.format(content))
                print 'get_bucket error {0}'.format(content)
            elif response['status'] == '200':
                bucketInfo = RestParser().parse_get_bucket_response(content)
        except socket.error:
            raise ServerUnavailableException(ip=self.ip)
        except httplib2.ServerNotFoundError:
            raise ServerUnavailableException(ip=self.ip)
        return bucketInfo

    def get_vbuckets(self, bucket='default'):
        bucket_info = self.get_bucket(bucket)
        #TODOL either bucket does not exist or user is not authenticated
        if not bucket_info:
            raise Exception("bucket {0} does not exist or the given bucket password is invalid".format(bucket))
        return self.get_bucket(bucket).vbuckets


class MembaseServerVersion:
    def __init__(self, implementationVersion='', componentsVersion=''):
        self.implementationVersion = implementationVersion
        self.componentsVersion = componentsVersion

#this class will also contain more node related info
class OtpNode(object):
    def __init__(self, id='', status=''):
        self.id = id
        self.ip = ''
        self.replication = ''
        self.port = 8091
        #extract ns ip from the otpNode string
        #its normally ns_1@10.20.30.40
        if id.find('@') >= 0:
            self.ip = id[id.index('@') + 1:]
        self.status = status


class NodeInfo(object):
    def __init__(self):
        self.availableStorage = None # list
        self.memoryQuota = None


class NodeDataStorage(object):
    def __init__(self):
        self.type = '' #hdd or ssd
        self.path = ''
        self.quotaMb = ''
        self.state = '' #ok

    def __str__(self):
        return '{0}'.format({'type': self.type,
                             'path': self.path,
                             'quotaMb': self.quotaMb,
                             'state': self.state})


class NodeDiskStorage(object):
    def __init__(self):
        self.type = 0
        self.path = ''
        self.sizeKBytes = 0
        self.usagePercent = 0


class Bucket(object):
    def __init__(self):
        self.name = ''
        self.port = 11211
        self.type = ''
        self.nodes = None
        self.stats = None
        self.servers = []
        self.vbuckets = []
        self.forward_map = []
        self.numReplicas = 0
        self.saslPassword = ""
        self.authType = ""


class Node(object):
    def __init__(self):
        self.uptime = 0
        self.memoryTotal = 0
        self.memoryFree = 0
        self.mcdMemoryReserved = 0
        self.mcdMemoryAllocated = 0
        self.status = ""
        self.hostname = ""
        self.clusterCompatibility = ""
        self.version = ""
        self.os = ""
        self.ports = []
        self.availableStorage = []
        self.storage = []
        self.memoryQuota = 0
        self.moxi = 11211
        self.memcached = 11210
        self.id = ""
        self.ip = ""
        self.rest_username = ""
        self.rest_password = ""


class AutoFailoverSettings(object):
    def __init__(self):
        self.enabled = True
        self.timeout = 0
        self.count = 0


class NodePort(object):
    def __init__(self):
        self.proxy = 0
        self.direct = 0


class BucketStats(object):
    def __init__(self):
        self.quotaPercentUsed = 0
        self.opsPerSec = 0
        self.diskFetches = 0
        self.itemCount = 0
        self.diskUsed = 0
        self.memUsed = 0
        self.ram = 0


class vBucket(object):
    def __init__(self):
        self.master = ''
        self.replica = []
        self.id = -1


class RestParser(object):
    def __init__(self):
        pass
        #self.log = logger
    def parse_get_nodes_response(self, parsed):
        node = Node()
        node.uptime = parsed['uptime']
        node.memoryFree = parsed['memoryFree']
        node.memoryTotal = parsed['memoryTotal']
        node.mcdMemoryAllocated = parsed['mcdMemoryAllocated']
        node.mcdMemoryReserved = parsed['mcdMemoryReserved']
        node.status = parsed['status']
        node.hostname = parsed['hostname']
        node.clusterCompatibility = parsed['clusterCompatibility']
        node.version = parsed['version']
        node.os = parsed['os']
        if "otpNode" in parsed:
            node.id = parsed["otpNode"]
            if parsed["otpNode"].find('@') >= 0:
                node.ip = node.id[node.id.index('@') + 1:]

        # memoryQuota
        if 'memoryQuota' in parsed:
            node.memoryQuota = parsed['memoryQuota']
        if 'availableStorage' in parsed:
            availableStorage = parsed['availableStorage']
            for key in availableStorage:
                #let's assume there is only one disk in each noce
                dict = parsed['availableStorage']
                if 'path' in dict and 'sizeKBytes' in dict and 'usagePercent' in dict:
                    diskStorage = NodeDiskStorage()
                    diskStorage.path = dict['path']
                    diskStorage.sizeKBytes = dict['sizeKBytes']
                    diskStorage.type = key
                    diskStorage.usagePercent = dict['usagePercent']
                    node.availableStorage.append(diskStorage)
                    #self.log.info(diskStorage)

        if 'storage' in parsed:
            storage = parsed['storage']
            for key in storage:
                disk_storage_list = storage[key]
                for dict in disk_storage_list:
                    if 'path' in dict and 'state' in dict and 'quotaMb' in dict:
                        dataStorage = NodeDataStorage()
                        dataStorage.path = dict['path']
                        dataStorage.quotaMb = dict['quotaMb']
                        dataStorage.state = dict['state']
                        dataStorage.type = key
                        node.storage.append(dataStorage)

        # ports":{"proxy":11211,"direct":11210}
        if "ports" in parsed:
            ports = parsed["ports"]
            if "proxy" in ports:
                node.moxi = ports["proxy"]
            if "direct" in ports:
                node.memcached = ports["direct"]
        return node

    def parse_get_bucket_response(self, response):
        parsed = json.loads(response)
        return self.parse_get_bucket_json(parsed)

    def parse_get_bucket_json(self, parsed):
        bucket = Bucket()
        bucket.name = parsed['name']
        bucket.type = parsed['bucketType']
        bucket.port = parsed['proxyPort']
        bucket.authType = parsed["authType"]
        bucket.saslPassword = parsed["saslPassword"]
        bucket.nodes = list()
        if 'vBucketServerMap' in parsed:
            vBucketServerMap = parsed['vBucketServerMap']
            serverList = vBucketServerMap['serverList']
            bucket.servers.extend(serverList)
            if "numReplicas" in vBucketServerMap:
                bucket.numReplicas = vBucketServerMap["numReplicas"]
                #vBucketMapForward
            if 'vBucketMapForward' in vBucketServerMap:
                #let's gather the forward map
                vBucketMapForward = vBucketServerMap['vBucketMapForward']
                for vbucket in vBucketMapForward:
                    #there will be n number of replicas
                    vbucketInfo = vBucket()
                    vbucketInfo.master = serverList[vbucket[0]]
                    if vbucket:
                        for i in range(1, len(vbucket)):
                            if vbucket[i] != -1:
                                vbucketInfo.replica.append(serverList[vbucket[i]])
                    bucket.forward_map.append(vbucketInfo)
            vBucketMap = vBucketServerMap['vBucketMap']
            counter = 0
            for vbucket in vBucketMap:
                #there will be n number of replicas
                vbucketInfo = vBucket()
                vbucketInfo.master = serverList[vbucket[0]]
                if vbucket:
                    for i in range(1, len(vbucket)):
                        if vbucket[i] != -1:
                            vbucketInfo.replica.append(serverList[vbucket[i]])
                vbucketInfo.id = counter
                counter += 1
                bucket.vbuckets.append(vbucketInfo)
                #now go through each vbucket and populate the info
                #who is master , who is replica
            # get the 'storageTotals'
        #self.log.debug('read {0} vbuckets'.format(len(bucket.vbuckets)))
        stats = parsed['basicStats']
        #vBucketServerMap
        bucketStats = BucketStats()
        #self.log.debug('stats:{0}'.format(stats))
        bucketStats.quotaPercentUsed = stats['quotaPercentUsed']
        bucketStats.opsPerSec = stats['opsPerSec']
        if 'diskFetches' in stats.keys():
            bucketStats.diskFetches = stats['diskFetches']
        if 'diskUsed' in stats.keys():
            bucketStats.diskUsed = stats['diskUsed']
        bucketStats.itemCount = stats['itemCount']
        bucketStats.memUsed = stats['memUsed']
        quota = parsed['quota']
        bucketStats.ram = quota['ram']
        bucket.stats = bucketStats
        nodes = parsed['nodes']
        for nodeDictionary in nodes:
            node = Node()
            node.uptime = nodeDictionary['uptime']
            node.memoryFree = nodeDictionary['memoryFree']
            node.memoryTotal = nodeDictionary['memoryTotal']
            node.mcdMemoryAllocated = nodeDictionary['mcdMemoryAllocated']
            node.mcdMemoryReserved = nodeDictionary['mcdMemoryReserved']
            node.status = nodeDictionary['status']
            node.hostname = nodeDictionary['hostname']
            if 'clusterCompatibility' in nodeDictionary:
                node.clusterCompatibility = nodeDictionary['clusterCompatibility']
            node.version = nodeDictionary['version']
            node.os = nodeDictionary['os']
            if "ports" in nodeDictionary:
                ports = nodeDictionary["ports"]
                if "proxy" in ports:
                    node.moxi = ports["proxy"]
                if "direct" in ports:
                    node.memcached = ports["direct"]
            if "hostname" in nodeDictionary:
                value = str(nodeDictionary["hostname"])
                node.ip = value[:value.rfind(":")]
                node.port = int(value[value.rfind(":") + 1:])
            if "otpNode" in nodeDictionary:
                node.id = nodeDictionary["otpNode"]
            bucket.nodes.append(node)
        return bucket
