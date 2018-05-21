import socket
from threading import Thread, Condition
import math, sys, zmq
from math import pow
import traceback, time
from copy import deepcopy
import numpy

IP = ""
ZMQ_LINK_PORT = 0
ZMQ_APP_PORT = 0
rrep_map = {}
rrep_calls = {}
forward = {}
condition = Condition()
app_cond = Condition()
test_msg = ""
recv_node_flag = False

PREQCOUNT = 1

def link_server_display(nl):
    zmq_sock = nl.zmq_link
    print "asd"
    #zmq_sock.send("asd")        # CONNECT
    while True:
        try:
            message = zmq_sock.recv()
            message = message.split()
            if message[0] == "DIRECT":
                print message
                if message[2] == "PREQ":
                    message = message[3:]
                    nl.recvPreqMessage(message[0],message[1],message[2],message[3])
                elif message[2] == "RREP":
                    message = message[3:]
                    nl.recvRrepMessage(message[0])
                elif message[2] == "MESSAGE":
                    message = message[3:]
                    #condition.acquire()
                    nl.recvMessage(message)
                    """print "From " + source + " message: " + msg
                    global test_msg
                    test_msg = msg
                    condition.notify()
                    condition.release()
                    if recv_node_flag:
                        print "data sent"
                        nl.sendDataTry(source, msg)"""

        except:
            print "ERROR"
            print traceback.format_exc()
            pass
class NetworkLayer:
    def __init__(self):
        #init zmq socket to link layer
        self.context = zmq.Context()
        self.zmq_link = self.context.socket(zmq.PAIR)
        self.zmq_link.connect("tcp://127.0.0.1:%s" % ZMQ_LINK_PORT)
        self.zmq_t = Thread(target=link_server_display,args=(self,))
        self.zmq_t.start()

        #init zmq socket to app layer
        self.context = zmq.Context()
        self.zmq_socket = self.context.socket(zmq.PAIR)
        self.zmq_socket.bind("tcp://127.0.0.1:%s" % ZMQ_APP_PORT)


    def sendDirectMessage(self, destination, message):
        self.zmq_link.send("DIRECT " + destination + " " + message)
        return

    def sendBroadCast(self,message):
        self.zmq_link.send("BROADCAST " + message)

    def sendPreqMessage(self,destination,id):
        tmp_str = "PREQ " + str(IP) + " " + destination + " " + str(id) + " _"
        self.sendBroadCast(tmp_str)

    def recvPreqMessage(self, source, destination, msg_id, path):
        print source, destination, msg_id, path
        if source not in rrep_map or (source in rrep_map and rrep_map[source] < msg_id):
            rrep_map[source] = msg_id
            if path != "_":
                msg = "RREP " + source + "|" + path + "|" + IP
                path = source + "|" + path + "|" + IP
                self.sendDirectMessage(path.split("|")[-1], msg)
            else:
                msg = "RREP " + source + "|" + IP
                path = source + "|" + IP
                self.sendDirectMessage(source, msg)
        else:
            pass
    def recvRrepMessage(self, path):
        forward[path.split("|")[-1]] = path
        print path
        print "calling"
        dest = path.split("|")[-1]
        if dest in rrep_calls:
            for call in rrep_calls[dest]:
                call()

    def recvMessage(self,msg):
        source = msg[0].split("|")[0]
        msg = " ".join(msg[1:])
        app_cond.acquire()
        self.zmq_socket.send(source + " " + msg)
        app_cond.notify()
        app_cond.release()

    def sendData(self,destination,msg):
        print "called with " + destination + " " + msg
        print "gateway " + forward[destination].split("|")[1] + " path " + forward[destination] + " msg " + msg
        self.sendDirectMessage(forward[destination].split("|")[1], "MESSAGE " + forward[destination] + " " + msg)

    def sendBenchMark(self,destination,data):
        self.sendDataTry(destination,data)
        condition.acquire()
        condition.wait()
        print "Test: " + test_msg
        msg = deepcopy(test_msg)
        print "MSG: " + msg
        condition.release()
        return msg

    def sendDataTry(self, destination, msg):
        if destination not in forward:
            if destination not in rrep_calls:
                rrep_calls[destination] = [lambda: NetworkLayer.sendData(self,destination, msg)]
                global PREQCOUNT
                PREQCOUNT += 1
                self.sendPreqMessage(destination,PREQCOUNT)
            else:
                rrep_calls[destination] += [lambda: NetworkLayer.sendData(self,destination, msg)]
        else:
            self.sendData(destination,msg)

    def recvBenchmark(self):
        global recv_node_flag
        recv_node_flag = True
        #source, data = self.recvMessage()
        #print "recieved data: " + data + " from " + source
        #self.sendData(source, data)

    def run2(self):
        while True:
            user_inp = None
            try:
                app_cond.acquire()
                user_inp = self.zmq_socket.recv(flags=zmq.NOBLOCK)
            except:
                pass
            app_cond.notify()
            app_cond.release()
            if user_inp:
                try:
                    inp = user_inp.split()
                    tmp_ind = user_inp.find(" ")
                    dst = user_inp[0:tmp_ind]
                    msg = user_inp[tmp_ind+1:]
                    self.sendDataTry(dst,msg)
                except zmq.Again as e:
                    pass
            #app_cond.notify()
            #app_cond.release()
    def run(self):
        while True:
            user_inp = raw_input()
            inp = user_inp.split()
            if inp[0] == "PREQ":
                self.sendPreqMessage(inp[1],int(inp[2]))#self.zmq_link.send("DIRECT " + inp[1] + " " + " ".join(inp[2:]) )
            elif inp[0] == "B":
                self.zmq_link.send("BROADCAST " + " " + " ".join(inp[1:]) )
            elif inp[0] == "DATA":
                self.sendDataTry(inp[1],inp[2])
            elif inp[0] == "TEST":
                rep = int(inp[2])
                time_arr = []
                while rep > 0:
                    data = "aaaaaaaaaa"
                    start = time.time()
                    recvData = nl.sendBenchMark(inp[1], data)
                    print recvData
                    end = time.time()
                    result = data == recvData
                    time_arr.append((end-start) * 2000000.0)
                    print str((end-start) * 2000000.0) + " microsecond data is " + "correct" if result else "INCORRECT"
                    rep -= 1
                print "With PREQ: " + str(time_arr[0])
                print "Avg of non PREQ " + str( numpy.mean(time_arr[1:]) ) #str( sum(time_arr[1:]) / (len(time_arr)-1) )
            elif inp[0] == "exit":
                pass

def createBencmarkData(mb):
    data = "a" * 1000000 * mb
    return data

""" Usage:
        bridge node: IP Port
        sender: IP Port 'send' destIp
        reciver: IP Port 'recv'
"""

if __name__ == "__main__":
    IP = str(sys.argv[1])
    ZMQ_LINK_PORT = int(sys.argv[2])
    ZMQ_APP_PORT = int(sys.argv[3])
    nl = NetworkLayer()
    if len(sys.argv) == 4:
        nl.run2()
    elif sys.argv[4] == "send":
        #data = createBencmarkData(int(sys.argv[6]))
        nl.run()
    elif sys.argv[4] == "recv":
        nl.recvBenchmark()