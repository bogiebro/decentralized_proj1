import xmlrpc.client
import xmlrpc.server
from socketserver import ThreadingMixIn
from xmlrpc.server import SimpleXMLRPCServer
from rwlock import RWLockDict, RWLock
import random
import socket
import concurrent.futures as futures
from contextlib import contextmanager
import logging
from threading import Thread
import time

kvsServers = dict()
baseAddr = "http://localhost:"
baseServerPort = 9000

class SimpleThreadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
  pass

def tag(fut, id):
  fut.server_id = id
  return fut

def sort_results(results):
  failure = results.not_done
  success = []
  for b in results.done:
    if b.exception(timeout=0) is None:
      success.append(b)
    else:
      failure.append(b)
  return success, failure

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
          for id, s in self.servers.items()], timeout=1)
        success, failure = sort_results(results)
        self.server_ids = [b.server_id for b in success]
        for b in failure:
          del self.servers[b.server_id]
    return ""

  def heartbeat(self):
    with futures.ThreadPoolExecutor() as ex:
        results = futures.wait(
          [tag(ex.submit(lambda : s.beat()), id)
          for id, s in self.servers.items()], timeout=0.5)
        self.server_ids = [b.server_id for b in results.done]
        for b in results.not_done:
          del self.servers[b.server_id]

  def get(self, key):
    print("Frontend getting key", key)
    with self.lockdict.r_locked(key):
      return self.with_rand_server(lambda id:
        self.servers[id].get(key), "ERR_KEY")

  # NOTE: this isn't thread safe.
  # The server id list needs to be locked while we get a
  # random value. Also: why do we maintain a separate id list?
  # Why not just maintain the dict?
  def with_rand_server(self, f, default):
    if len(self.server_ids) == 0:
      return default
    id_ix = random.randint(0, len(self.server_ids) - 1)
    id = self.server_ids[id_ix]
    with futures.ThreadPoolExecutor() as ex:
      while True:
        fut = ex.submit(f, id)
        try:
          return fut.result(timeout=1)
        except (TimeoutError, ConnectionRefusedError):
          self.server_ids[id_ix] = self.server_ids[-1]
          self.server_ids.pop()
          del self.servers[id]
          if len(self.server_ids) == 0:
            return default
          id_ix = random.randint(0, len(self.server_ids))
          id = self.server_ids[id_ix]

  def printKVPairs(self, serverId):
    if serverId not in self.servers:
      return "ERR_NOEXIST"
    with self.lockdict.all_locked():
      try:
        store = self.servers[serverId].getAll()
        return "\n".join(f"{k}: {v}" for k, v in store.items())
      except (TimeoutError, ConnectionRefusedError):
        return "ERR_NOEXIST"

  def addServer(self, serverId):
    logging.info(f"Connecting to {baseServerPort + serverId}") 
    self.servers[serverId] = xmlrpc.client.ServerProxy(
      baseAddr + str(baseServerPort + serverId),
      allow_none=True, use_builtin_types=True)
    if len(self.servers) > 0:
      with self.lockdict.all_locked():
        store = self.with_rand_server(lambda id:
          self.servers[id].getAll(), "")
        if len(store) > 0:
          self.servers[serverId].putAll(store)
    self.server_ids.append(serverId)
    return ""

  def listServer(self):
    if len(self.server_ids) == 0:
      return "ERR_NOSERVERS"
    return ", ".join(map(repr, sorted(self.server_ids)))

  def shutdownServer(self, serverId):
    result = self.servers[serverId].shutdownServer()
    del self.servers[serverId]
    return result

logging.basicConfig(filename="frontend.log", level=logging.DEBUG)
server = SimpleThreadedXMLRPCServer(("localhost", 8001))
rpc = FrontendRPCServer()
server.register_instance(rpc)

def hearbeat_loop():
  while True:
    Thread(target= lambda: rpc.heartbeat()).start()
    time.sleep(0.5)

# Thread(target=hearbeat_loop).start()
server.serve_forever()
