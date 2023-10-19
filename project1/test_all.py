import subprocess as sp
import logging
import xmlrpc.client
import random
import time
import pytest
import concurrent.futures as futures

baseAddr = "http://localhost:"
baseClientPort = 7000
baseFrontendPort = 8001
baseServerPort = 9000

def makeClient():
  return xmlrpc.client.ServerProxy(baseAddr + str(baseClientPort))

def makeFrontend():
  return xmlrpc.client.ServerProxy(baseAddr + str(baseFrontendPort))

@pytest.fixture(scope="session")
def client_proc():
    client = sp.Popen(["python", "client.py", "-i", str(0)])
    time.sleep(1)
    yield makeClient 
    client.kill()

@pytest.fixture(scope="session")
def frontend_proc():
    front_thread = sp.Popen(["python", "frontend.py"])
    time.sleep(1)
    yield makeFrontend
    front_thread.kill()

class ServerList:
  def __init__(self):
    self.map = dict()
    self.next = 0

@pytest.fixture(scope="function")
def servers():
  s = ServerList()
  yield s
  for v in s.map.values():
    v.kill()
  for v in s.map.values():
    v.wait()




def addServer(frontend, servers):
    id = servers.next
    servers.next += 1
    servers.map[id] = sp.Popen(["python", "server.py", "-i", str(id)])
    time.sleep(1)
    frontend.addServer(id)

def listServer(frontend):
    return frontend.listServer()

def killServer(servers, serverId):
    servers.map[serverId].kill()
    time.sleep(1)

def shutdownServer(frontend, serverId):
    frontend.shutdownServer(serverId)

def printKVPairs(frontend, serverId):
    return frontend.printKVPairs(serverId)

def test_kvstore(frontend_proc, servers, client_proc):
  client = client_proc()
  frontend = frontend_proc()
  for _ in range(6):
    addServer(frontend, servers)
  client.put("hey", "jude")
  for _ in range(10):
    assert client.get("hey") == "hey:jude"

def test_monotonic(frontend_proc, servers, client_proc):
  frontend = frontend_proc()
  client = client_proc()
  for _ in range(6):
    addServer(frontend, servers)
  tokill = iter(range(6))
  def runner():
    val = client.get("hey")
    if val == "ERR_KEY":
      return
    client.put("hey", int(val.split(":")[1]) + 1)
    try:
      if random.random() < 0.1:
        killServer(next(tokill), tokill)
    except StopIteration:
      pass
    val2 = client.get("hey")
    if val != "ERR_KEY":
      assert int(val.split(":")[1]) >= (val + 1)
  futs = []
  with futures.ThreadPoolExecutor() as ex:
    for _ in range(50):
      for fut in futures.as_completed(futs):
        futs.append(ex.submit(runner))
  for fut in futures.as_completed(futs):
    fut.result()

def test_heartbeat(frontend_proc, servers):
    frontend = frontend_proc()
    addServer(frontend, servers)
    addServer(frontend, servers)
    assert listServer(frontend) == "0, 1"
    killServer(servers, 0)
    time.sleep(1)
    assert listServer(frontend) == "1"

def test_conc_reads(frontend_proc, servers, client_proc):
  frontend = frontend_proc()
  client = client_proc()
  addServer(frontend, servers)
  client.put("key", 5)
  a = timeReads(client_proc)
  multicall = xmlrpc.client.MultiCall(frontend)
  for i in range(6):
    addServer(multicall, servers)
  _ = multicall()
  b = timeReads(client_proc)
  assert b < a

def timeReads(client_proc):
  start_time = time.time()
  futs = []
  with futures.ThreadPoolExecutor() as ex:
    for _ in range(100):
      futs.append(ex.submit(lambda : client_proc().get("key")))
  for fut in futures.as_completed(futs):
      fut.result()
  end_time = time.time()
  return end_time - start_time

