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
from sortedcontainers import SortedSet
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
    self.servers = SortedSet()
    self.serverlock = RWLock()

  def put(self, key, value):
    if len(self.servers) == 0:
      return "ERR_NOEXIST"
    with futures.ThreadPoolExecutor() as ex:
      with self.lockdict.w_locked(key):
        with self.serverlock.r_locked():
          jobs = [tag(ex.submit(lambda : connect(id).put(key, value)), id)
            for id in self.servers]
        results = futures.wait(jobs, timeout=1)
        failures = get_failed(results)
        for b in failures:
            try:
              with self.serverlock.w_locked():
                self.servers.discard(b.server_id)
            except KeyError:
              pass
    return ""

  def get(self, key):
    with self.lockdict.r_locked(key):
      return self.with_rand_server(lambda id:
        connect(id).get(key), "ERR_KEY")

  def with_rand_server(self, f, default):
    with futures.ThreadPoolExecutor() as ex:
      while True:
        if len(self.servers) == 0:
          return default
        n = random.randint(0, len(self.servers) - 1)
        try:
          id = self.servers[n]
        except IndexError:
          continue
        fut = ex.submit(f, id)
        try:
          return fut.result(timeout=1)
        except (TimeoutError, ConnectionRefusedError):
          try:
            with self.serverlock.w_locked():
              self.servers.discard(id)
          except KeyError:
            pass

  def printKVPairs(self, id):
    if id not in self.servers:
      return "ERR_NOEXIST"
    with self.lockdict.all_locked():
      try:
        store = connect(id).getAll()
        return "\n".join(f"{k}: {v}" for k, v in store.items())
      except (TimeoutError, ConnectionRefusedError):
        return "ERR_NOEXIST"

  def addServer(self, serverId):
    # logging.info(f"Connecting to {baseServerPort + serverId}") 
    # if len(self.servers) > 0:
    #   server = connect(serverId)
    #   with self.lockdict.all_locked():
    #     store = self.with_rand_server(lambda s: connect(s).getAll(), {})
    #     if len(store) > 0:
    #       server.putAll(store)
    self.servers.add(serverId)
    return "OK"

  def listServer(self):
    if len(self.servers) == 0:
      return "ERR_NOSERVERS"
    with self.serverlock.r_locked():
      return ", ".join(map(repr, self.servers))

  def shutdownServer(self, serverId):
    try:
      result = connect(serverId).shutdownServer()
      with self.serverlock.w_locked():
        self.servers.discard(result)
    except KeyError:
      pass
    return ""

def connect(serverId):
  return xmlrpc.client.ServerProxy(
      baseAddr + str(baseServerPort + serverId),
      allow_none=True, use_builtin_types=True)

def heartbeat(frontend):
  with futures.ThreadPoolExecutor() as ex:
      with frontend.serverlock.r_locked():
        jobs = [tag(ex.submit(lambda : connect(id).beat()), id)
          for id in frontend.servers]
      results = futures.wait(jobs, timeout=1)
      failures = get_failed(results)
      for b in failures:
        try:
          with frontend.serverlock.w_locked():
            frontend.servers.discard(b.server_id)
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

# Thread(target=hearbeat_loop).start()
server.serve_forever()
