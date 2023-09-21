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

@pytest.fixture(scope="module")
def client():
    client = sp.Popen(["python", "client.py", "-i", str(0)])
    time.sleep(1)
    yield xmlrpc.client.ServerProxy(baseAddr + str(baseClientPort))
    client.kill()

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

@pytest.fixture(scope="module")
def frontend():
    sp.run(["sh", "killer.sh"])
    front_thread = sp.Popen(["python", "frontend.py"])
    time.sleep(1)
    yield xmlrpc.client.ServerProxy(baseAddr + str(baseFrontendPort))
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

def test_kvstore(frontend, servers, client):
  for _ in range(6):
    addServer(frontend, servers)
  client.put("hey", "jude")
  for _ in range(10):
    assert client.get("hey") == "jude"

def test_monotonic(frontend, servers, client):
  for _ in range(6):
    addServer(frontend, servers)
  tokill = iter(range(6))
  def runner():
    val = client.get("hey")
    if val == "ERR_KEY":
      return
    client.put("hey", val + 1)
    try:
      if random.random() < 0.1:
        killServer(next(tokill), tokill)
    except StopIteration:
      pass
    val2 = client.get("hey")
    if val != "ERR_KEY":
      assert val >= (val + 1)
  with futures.ThreadPoolExecutor() as ex:
    for _ in range(500):
      ex.submit(runner)

def test_heartbeat(frontend, servers):
    addServer(frontend, servers)
    addServer(frontend, servers)
    assert listServer(frontend) == "0, 1"
    killServer(servers, 0)
    time.sleep(1)
    assert listServer(frontend) == "1"

def test_conc_reads(frontend, servers, client):
  addServer(frontend, servers)
  client.put("key", 5)
  a = timeReads(client)
  multicall = xmlrpc.client.MultiCall(frontend)
  for i in range(6):
    addServer(frontend, servers)
  _ = multicall()
  b = timeReads(client)
  assert b < a

def timeReads(client):
  start_time = time.time()
  with futures.ThreadPoolExecutor() as ex:
    for _ in range(500):
      ex.submit(lambda : client.get(key))
  end_time = time.time()
  return end_time - start_time

