import xmlrpc.client
import xmlrpc.server
from socketserver import ThreadingMixIn
from xmlrpc.server import SimpleXMLRPCServer
from rwlock import RWLockDict, RWLock
import random
import socket
import concurrent.futures as futures
from contextlib import contextmanager

kvsServers = dict()
baseAddr = "http://localhost:"
baseServerPort = 9000

socket.setdefaulttimeout(4)  

class SimpleThreadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
        pass

def tag(fut, id):
  fut.server_id = id
  return fut

class FrontendRPCServer:
  def __init__(self):
    self.lockdict = RWLockDict()
    self.servers = dict()
    self.server_ids = []

  def put(self, key, value):
    if len(self.server_ids) == 0:
      return "ERR_NOEXIST"
    with futures.ThreadPoolExecutor() as ex:
      with self.lockdict.w_locked(key):
        results = futures.wait(
          [tag(ex.submit(lambda : s.put(key, value)), id)
          for id, s in self.servers.items()])
        self.server_ids = [b.server_id for b in results.done]
        for b in results.not_done:
          del self.servers[b.server_id]

  def get(self, key):
    if len(self.server_ids) == 0:
      return "ERR_NOEXIST"
    with self.lockdict.r_locked(key):
      return self.with_rand_server(lambda id:
        self.servers[id].get(key))

  def with_rand_server(self, f):
    id_ix = random.randint(0, len(self.server_ids) - 1)
    id = self.server_ids[id_ix]
    with futures.ThreadPoolExecutor() as ex:
      while True:
        fut = ex.submit(f, id)
        try:
          return fut.result(timeout=2)
        except TimeoutError:
          self.server_ids[id_ix] = self.server_ids[-1]
          self.server_ids.pop()
          del self.servers[id]
          id_ix = random.randint(0, len(self.server_ids))
          id = self.server_ids[id_ix]

  def printKVPairs(self, serverId):
    if serverId not in self.servers:
      raise NoServerException()
    with self.lockdict.all_locked():
      return self.servers[serverId].printKVPairs()

  def addServer(self, serverId):
    self.servers[serverId] = xmlrpc.client.ServerProxy(
      baseAddr + str(baseServerPort + serverId),
      allow_none=True, use_builtin_types=True)
    self.server_ids.append(serverId)

  def listServer(self):
    if len(self.server_ids) == 0:
      return "ERR_NOSERVERS"
    return ", ".join(map(repr, sorted(self.server_ids)))

  def shutdownServer(self, serverId):
    result = self.servers[serverId].shutdownServer()
    del self.servers[serverId]
    return result

server = SimpleThreadedXMLRPCServer(("localhost", 8001))
server.register_instance(FrontendRPCServer())

server.serve_forever()

# server = FrontendRPCServer()
# server.addServer(0)
# server.put("hey", "jude")
# print(server.listServer())
# print(server.get("dude"))
# print(server.printKVPairs(0))
