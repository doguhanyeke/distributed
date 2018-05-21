import socket
from threading import Thread, Lock
import math, sys, zmq
from math import pow


ZMQ_PORT = 0
BROADCAST_IP = "10.255.255.255"
BROADCAST_PORT = 5005
DIRECT_PORT = 5006
DISTANCE = 25
nb = []
broadcasted_msg = {}

zmq_lock = Lock()

def linklayer_controller(ip,x,y):
    lc = LinkLayerClient(ip,x,y)
    ls = LinkLayerServer(ip,x,y,lc)
    ls.
    ()


class LinkLayerServer:
    def __init__(self, IP, x, y,lc):
        self.IP = IP
        self.x = x
        self.y = y
        self.lc = lc
        #init zmq socket
        self.context = zmq.Context()
        self.zmq_socket = self.context.socket(zmq.PAIR)
        self.zmq_socket.bind("tcp://*:%s" % ZMQ_PORT)
        #self.zmq_socket.recv();                 # !!! ACCEPT NETWORK LAYER

        #init direct socket
        self.direct_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.direct_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.direct_sock.setblocking(0)
        self.direct_sock.bind( ("0.0.0.0", DIRECT_PORT) )

        # DISCOVER THREAD
        self.discover_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.discover_sock.bind( ("0.0.0.0", BROADCAST_PORT) )
        self.discover_sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.discover_thread = Thread(target=LinkLayerServer.discover,args=(self,))
        self.discover_thread.start()
        # DISCOVER THREAD


    def discover(self):
        self.lc.discover()
        while True:
            print " b4 rcv discover msg"
            msg, address = self.discover_sock.recvfrom(1024)
            print "Discover Thread received: ", msg, " from ", address
            print msg
            msg = msg.split()

            if len(msg) == 2:
                #print "discover cevabi geldi adres: ", address
                ip = msg[1]
            else:
                #print "discover msg geldi adres: ", address
                x,y,ip = msg
                z = pow( pow( int(x)-self.x,2) + pow( int(y) - self.y, 2), 0.5 )
                print z
                if ip != self.IP and z < DISTANCE:
                    self.discover_sock.sendto("DISCOVER_MESSAGE " + self.IP, (ip, BROADCAST_PORT) )
                else:
                    continue
            if (ip != self.IP) and (ip not in nb):
                nb.append(ip)
                print "sending b4 zmq ", ip
                zmq_lock.acquire()
                #self.zmq_socket.send("Received IP: " + ip)
                print "sending after zmq2 ", ip
                zmq_lock.release()

    def main_fnc(self):
        while True:
            #print "Are you in honey"
            try:
                #check for a message, this will not block
                zmq_lock.acquire()
                message = self.zmq_socket.recv(flags=zmq.NOBLOCK)
                print "Message from network layer is %s" % message
                msg_list = message.split()
                if(msg_list[0] == 'DIRECT'):
                    print " ".join(msg_list[2:])
                    self.lc.direct(msg_list[1], " ".join(msg_list[2:]))
                elif(msg_list[0] == "RECV"):
                    print "will recv " + msg_list[1]
                else:
                    if msg_list[1] == "PREQ" and msg_list[3] in nb:
                        self.lc.direct(msg_list[3], " ".join(msg_list[1:]))
                    else:
                        self.lc.broadcast(" ".join(msg_list[1:]))
            except zmq.Again as e:
                pass
            finally:
                zmq_lock.release()
                msg = None
            try:
                msg, address = self.direct_sock.recvfrom(1024)
                if not msg:
                    continue
                msg_list = msg.split()
                print msg
                if msg_list[0] == "PREQ":
                    if msg_list[2] == self.IP:
                        self.zmq_socket.send("DIRECT " + msg_list[2] + " " + msg)
                        continue
                    print "Conveying msg: %s" % msg
                    msg_key = "->".join(msg_list[1:3])
                    if msg_key not in broadcasted_msg or broadcasted_msg[msg_key] < int(msg_list[3]):
                        broadcasted_msg[msg_key] = int(msg_list[3])
                        print "-1 arg :" + msg_list[-1]
                        if msg_list[-1] == "_":
                            msg_list[-1] = self.IP
                        else:
                            msg_list[-1] += "|" + self.IP
                        if msg_list[2] in nb:
                            self.lc.direct(msg_list[2], " ".join(msg_list))
                        else:
                            self.lc.broadcast(" ".join(msg_list))
                    else:
                        pass
                elif msg_list[0] == "RREP":
                    print msg
                    path = msg_list[1].split("|")
                    ind = path.index(self.IP)
                    if ind == 0:
                        print "ind 0"
                        self.zmq_socket.send("DIRECT " + path[0] + " " + msg)
                    else:
                        print "conveyin rrep to " + path[ind-1]
                        self.lc.direct(path[ind-1],msg)
                elif msg_list[0] == "MESSAGE":
                    print msg
                    path = msg_list[1].split("|")
                    ind = path.index(self.IP)
                    if ind != len(path)-1:
                        self.lc.direct(path[ind+1], msg)
                    else:
                        self.zmq_socket.send("DIRECT " + self.IP + " " + msg)
            except socket.error:
                pass

class LinkLayerClient:
    def __init__(self, IP, x, y):
        self.IP = IP
        self.x = x
        self.y = y
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)


    def discover(self):
        print "broadcasting discover msg"
        msg = str(self.x) + " " + str(self.y) + " " + self.IP
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.sendto(msg, (BROADCAST_IP, BROADCAST_PORT))

    def direct(self, ip, msg):
        mlen = len(msg.encode("utf-8"))
        while mlen>0:
            tmp = self.sock.sendto(msg, (ip, DIRECT_PORT))
            mlen -= tmp

    def broadcast(self,msg):
        for n in nb:
            self.direct(n,msg)

if __name__ == "__main__":
    ZMQ_PORT = int(sys.argv[4])
    linklayer_controller(sys.argv[1],int(sys.argv[2]),int(sys.argv[3]))