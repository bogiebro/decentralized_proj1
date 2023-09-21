import xmlrpc.client
import xmlrpc.server
from socketserver import ThreadingMixIn
from xmlrpc.server import SimpleXMLRPCServer
from rwlock import RWLockDict, RWLock
import socket
import concurrent.futures as futures
from contextlib import contextmanager
import logging
from threading import Thread
import time
from sortedcontainers import SortedDict
import random

kvsServers = dict()
baseAddr = "http://localhost:"
baseServerPort = 9000

class SimpleThreadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
  pass

def tag(fut, id):
  fut.server_id = id
  return fut

def get_failed(results):
  failed = list(results.not_done)
  for b in results.done:
    if b.exception(timeout=0) is not None:
      failed.append(b)
  return failed

class FrontendRPCServer:
  def __init__(self):
    self.lockdict = RWLockDict()
    self.servers = SortedDict()

  def put(self, key, value):
    if len(self.servers) == 0:
      return "ERR_NOEXIST"
    with futures.ThreadPoolExecutor() as ex:
      with self.lockdict.w_locked(key):
        results = futures.wait(
          [tag(ex.submit(lambda : s.put(key, value)), id)
          for id, s in self.servers.items()], timeout=1)
        failures = get_failed(results)
        for b in failures:
            try:
              del self.servers[b.server_id]
            except KeyError:
              pass
    return ""

  def get(self, key):
    print("Frontend getting key", key)
    with self.lockdict.r_locked(key):
      return self.with_rand_server(lambda server:
        server.get(key), "ERR_KEY")

  def with_rand_server(self, f, default):
    with futures.ThreadPoolExecutor() as ex:
      while True:
        if len(self.servers) == 0:
          return default
        n = random.randint(0, len(self.servers) - 1)
        try:
          id, server = self.servers.peekitem(n)
        except IndexError:
          continue
        fut = ex.submit(f, server)
        try:
          return fut.result(timeout=1)
        except (TimeoutError, ConnectionRefusedError):
          try:
            del self.servers[id]
          except KeyError:
            pass

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
        store = self.with_rand_server(lambda s: s.getAll(), "")
        if len(store) > 0:
          self.servers[serverId].putAll(store)
    return "OK"

  def listServer(self):
    if len(self.servers) == 0:
      return "ERR_NOSERVERS"
    return ", ".join(map(repr, sorted(self.servers.keys())))

  def shutdownServer(self, serverId):
    try:
      result = self.servers[serverId].shutdownServer()
      del self.servers[serverId]
    except KeyError:
      pass
    return ""


def heartbeat(frontend):
  with futures.ThreadPoolExecutor() as ex:
      results = futures.wait(
        [tag(ex.submit(lambda : s.beat()), id)
        for id, s in frontend.servers.items()], timeout=0.5)
      failures = get_failed(results)
      for b in failures:
        try:
          del frontend.servers[b.server_id]
        except KeyError:
          pass


logging.basicConfig(filename="frontend.log", level=logging.DEBUG)
server = SimpleThreadedXMLRPCServer(("localhost", 8001))
server.register_multicall_functions()
rpc = FrontendRPCServer()
server.register_instance(rpc)

def hearbeat_loop():
  while True:
    Thread(daemon=True, target= lambda: heartbeat(rpc)).start()
    time.sleep(0.5)

Thread(target=hearbeat_loop).start()
server.serve_forever()
