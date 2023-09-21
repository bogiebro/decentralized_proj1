import subprocess as sp
import logging
import xmlrpc.client
import random
import time
import pytest

baseAddr = "http://localhost:"
baseClientPort = 7000
baseFrontendPort = 8001
baseServerPort = 9000

clientList = []
clientProcs = []

def addClient(clients):
    clientUID = len(clients.clientList)
    clients.clientProcs.append(sp.Popen(["python", "client.py", "-i", str(clientUID)]))
    time.sleep(1)
    clients.clientList.append(xmlrpc.client.ServerProxy(baseAddr + str(baseClientPort + clientUID)))

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

def put(clients, key, value):
    return clients.clientList[random.randint(0, len(clients.clientList) - 1)].put(key, value)

def get(clients, key):
    client = clients.clientList[random.randint(0, len(clients.clientList) - 1)]
    return client.get(key)

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

class ClientList:
  def __init__(self):
    self.clientProcs = []
    self.clientList = []

@pytest.fixture
def servers(scope="function"):
  s = ServerList()
  yield s
  for v in s.map.values():
    v.kill()
  for v in s.map.values():
    v.wait()

@pytest.fixture
def clients(scope="module"):
  s = ClientList()
  yield s
  for v in s.clientProcs:
    v.kill()

def test_kvstore(frontend, servers, clients):
  for _ in range(3):
    addServer(frontend, servers)
  addClient(clients)
  put(clients, "hey", "jude")
  assert get(clients, "hey") == "jude"

# def test_heartbeat(frontend, servers, clients):
#     addServer(frontend, servers)
#     addServer(frontend, servers)
#     assert listServer(frontend) == "0, 1"
#     killServer(servers, 0)
#     time.sleep(1)
#     assert listServer(frontend) == "1"

@pytest.mark.parametrize("n", [1, 2])
def test_conc_reads(frontend, servers, n):
  multicall = xmlrpc.client.MultiCall(frontend)
  for i in range(n):
    addServer(frontend, servers)
  _ = multicall()
  sp.run(["python", "clientWriter.py", "-v" "5"])
  procs = [sp.Popen(["python", "clientReader.py"]) for _ in range(500)]
  start_time = time.time()
  for p in procs:
    p.wait()
  end_time = time.time()
  logging.warning(f"Time with {n} {end_time - start_time}")

