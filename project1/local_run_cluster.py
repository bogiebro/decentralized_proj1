import subprocess as sp
import xmlrpc.client
import random
import time

baseAddr = "http://localhost:"
baseClientPort = 7000
baseFrontendPort = 8001
baseServerPort = 9000

SERVER_ID = 0

frontend = None
clientList = []
clientProcs = []
servers = dict()

def event_trigger():
    terminate = False
    while terminate != True:
        cmd = input("Enter a command: ")
        args = cmd.split(':')

        if args[0] == 'addClient':
            addClient()
        elif args[0] == 'addServer':
            addServer()
        elif args[0] == 'listServer':
            listServer()
        elif args[0] == 'killServer':
            serverId = int(args[1])
            killServer(serverId)
        elif args[0] == 'shutdownServer':
            serverId = int(args[1])
            shutdownServer(serverId)
        elif args[0] == 'put':
            key = int(args[1])
            value = int(args[2])
            put(key, value)
        elif args[0] == 'get':
            key = int(args[1])
            get(key)
        elif args[0] == 'printKVPairs':
            serverId = int(args[1])
            printKVPairs(serverId)
        elif args[0] == 'terminate':
            terminate = True
        else:
            print("Unknown command")

def addClient():
    clientUID = len(clientList)
    clientProcs.append(sp.Popen(["python", "client.py", "-i", str(clientUID)]))
    time.sleep(1)
    clientList.append(xmlrpc.client.ServerProxy(baseAddr + str(baseClientPort + clientUID)))

def addServer():
    global SERVER_ID
    servers[SERVER_ID] = sp.Popen(["python", "server.py", "-i", str(SERVER_ID)])
    time.sleep(1)
    frontend.addServer(SERVER_ID)
    SERVER_ID += 1

def listServer():
    result = frontend.listServer()
    print(result)

def killServer(serverId):
    servers[serverId].kill()
    time.sleep(1)

def shutdownServer(serverId):
    result = frontend.shutdownServer(serverId)
    print(result)

def put(key, value):
    result = clientList[random.randint(1, 100000) % len(clientList)].put(key, value)
    print(result)

def get(key):
    client = clientList[random.randint(1, len(clientList)) % len(clientList)]
    result = client.get(key)
    print(result)

def printKVPairs(serverId):
    result = frontend.printKVPairs(serverId)
    print(result)

def cleanup(front_thread):
    front_thread.kill()
    for s in servers.values():
      s.kill()
    for c in clientProcs:
      c.kill()

if __name__ == '__main__':
    front_thread = sp.Popen(["python", "frontend.py"])
    time.sleep(1)
    frontend = xmlrpc.client.ServerProxy(baseAddr + str(baseFrontendPort))
    # event_trigger()
    addServer()
    addServer()
    # addServer()
    # addClient()
    addClient()
    # put("hey", "dude")
    put("hey", "babe")
    # put("my", "truck")
    # get("hey")
    # get("hey")
    # get("truck")
    listServer()
    killServer(0)
    time.sleep(1)
    # get("hey")
    listServer()

    cleanup(front_thread)
