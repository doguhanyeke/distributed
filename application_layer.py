import zmq,sys
from threading import Timer
IP = ""
ZMQ_PORT = 0
ALGO_TYPE = ""
NEIGHBOR_IP = ""
LEADER_ID = -1
SENDER = None

SYSTEM = {1:"10.0.0.1",2:"10.0.0.2",3:"10.0.0.3",4:"10.0.0.4",5:"10.0.0.5"}

class ApplicationLayerBully:
    def __init__(self):
        context = zmq.Context()
        self.zmq_link = context.socket(zmq.PAIR)
        self.zmq_link.connect("tcp://127.0.0.1:%s" % ZMQ_PORT)
        self.t = None
        if SENDER:
            print "ima send now"
            self.elect()

    def send(self, msg):
        self.zmq_link.send(msg)

    def elect(self):
        for i in SYSTEM.keys():
            if LEADER_ID < i:
                print "sending to " + SYSTEM[i]
                self.send_leader_id(SYSTEM[i],LEADER_ID)
        if not self.t :
            print "start timer"
            self.t = Timer(3.0, self.commander)
            self.t.start()

    def commander(self, ):
        print "IM THE RULER OF ThE 7 WORLDS"
        for i in SYSTEM:
            if i != LEADER_ID:
                self.send(SYSTEM[i] + " commander " + str(LEADER_ID))
        print "FINITO"
        exit(0)

    def recv(self):
        while True:
            inp = self.zmq_link.recv()
            print "recieved " + inp
            inplist = inp.split()
            m_id = int(inplist[2])
            #print "m_id: " + str(m_id) + " LEADID " + str(LEADER_ID)
            if inplist[1] == "election":
                self.send(inplist[0]+ " OK " + str(LEADER_ID))
                if LEADER_ID == len(SYSTEM):
                    self.commander()
                    exit(0)
                self.elect()
            elif inplist[1] == "OK":
                self.t.cancel()
            elif inplist[1] == "commander":
                if int(inplist[2]) != LEADER_ID:
                    print "Found my man " + inplist[2]
            else:
                print "no control else"

    def send_leader_id( self, dest, leader_id):
        inp = dest + " election " + str(leader_id)
        print "sending to socket " + inp
        self.zmq_link.send(inp)

class ApplicationLayerRing:
    def __init__(self,):
        context = zmq.Context()
        self.zmq_link = context.socket(zmq.PAIR)
        self.zmq_link.connect("tcp://127.0.0.1:%s" % ZMQ_PORT)
        if SENDER:
            self.send_leader_id(NEIGHBOR_IP,LEADER_ID)

    def send(self, msg):
        self.zmq_link.send(msg)

    def recv(self):
        while True:
            inp = self.zmq_link.recv()
            print inp + " ilk print"
            inplist = inp.split()
            m_id = int(inplist[2])
            print "m_id: " + str(m_id) + " LEADID " + str(LEADER_ID)
            if inplist[1] == "election":
                if m_id > LEADER_ID:
                    print "sending to " + NEIGHBOR_IP + " with id " + inplist[2]
                    self.send(NEIGHBOR_IP + " election " + inplist[2])
                elif m_id < LEADER_ID:
                    print "sending to " + NEIGHBOR_IP + " with id " + str(LEADER_ID)
                    self.send(NEIGHBOR_IP + " election " + str(LEADER_ID) )
                else:
                    print "sending commander to" + NEIGHBOR_IP + " with " + str(LEADER_ID)
                    self.send(NEIGHBOR_IP + " commander " + str(LEADER_ID) )
            elif inplist[1] == "commander":
                if int(inplist[2]) != LEADER_ID:
                    self.send(NEIGHBOR_IP + " commander " + str(inplist[2]) )
                    print "Found my man " + inplist[2]
                else:
                    print "Commander msg come back to commander, exit!"
                    break
            else:
                print "no control else"

    def send_leader_id(self, dest, leader_id):
        inp = dest + " election " + str(leader_id)
        self.zmq_link.send(inp)

#python applayer.py 10.0.0.1 5100 sender ring ID NEIG
if __name__ == "__main__":
    IP = sys.argv[1]
    ZMQ_PORT = int(sys.argv[2])
    ALGO_TYPE = sys.argv[4]
    LEADER_ID = int(sys.argv[5])
    if ALGO_TYPE == "ring":
        NEIGHBOR_IP = sys.argv[6]
        SENDER = sys.argv[3] == "sender"
        app_ring = ApplicationLayerRing()
        app_ring.recv()
    elif ALGO_TYPE == "bully":
        SENDER = sys.argv[3] == "sender"
        app_ring = ApplicationLayerBully()
        app_ring.recv()