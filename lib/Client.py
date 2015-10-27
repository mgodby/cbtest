

from couchbaseClient import RestConnection
from couchbase import Couchbase

class ClusterException(Exception): pass
class ConnectionException(Exception): pass

class Cluster(object):
  def __init__(self, vip, port, username, password):
    self.kwargs = {
      "ip":   vip,
      "port": port,
      "username": username,
      "password": password,
    }
    self.client = RestConnection(self.kwargs)

  def get_buckets(self):
    """Function that return an array of all the buckets in a cluster"""
    try:
      buckets = []
      for bucket in self.client.get_buckets():
        buckets.append(bucket.name)
      return buckets
    except Exception, e:
      raise ClusterException(e)


class Connection(object):
    def __init__(self, vip, port, bucket, username, password):
      try:
        self.cb = Couchbase.connect(bucket=bucket, host=vip, port=port, username=username, password=password)
      except Exception, e:
        raise ConnectionException(e)

    def set(self, key, value, ttl=0):
      """Function to set a value in couchbase
         Requires: key, value and ttl (default is 0)"""
      try:
        return self.cb.set(key, value, ttl)
      except Exception, e:
        raise ConnectionException(e)

    def add(self, key, value, ttl=0):
      """Function to add a value in couchbase unless it already exists
         Requires: key, value and ttl (default is 0)"""
      try:
        return self.cb.add(key, value, ttl)
      except Exception, e:
        raise ConnectionException(e)

    def delete(self, key):
      """Function Remove the key-value entry for a given key in Couchbase
         Requires: key"""
      try:
        return self.cb.delete(key, quiet=True)
      except Exception, e:
        raise ConnectionException(e)

    def get(self, key):
      """Function to get a value in couchbase
         Requires: key """
      try:
        return self.cb.get(key)
      except Exception, e:
        raise ConnectionException(e)

