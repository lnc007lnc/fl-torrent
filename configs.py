"""configs in json format"""
import json

CFG = {
    "directory": {
        "logs_dir": "logs/",
        "node_files_dir": "node_files/",
        "tracker_db_dir": "tracker_db/"
    },
    "constants": {
        "AVAILABLE_PORTS_RANGE": (1024, 65535), # range of available ports on the local computer
        "TRACKER_ADDR": ('localhost', 12341),
        #"TRACKER_ADDR": ('0.0.0.0', 12345),
        "MAX_UDP_SEGMENT_DATA_SIZE": 65527,
        "BUFFER_SIZE": 9216,        # MACOSX UDP MTU is 9216
        "CHUNK_PIECES_SIZE": 9216 - 2000, # Each chunk pieces(segments of UDP) must be lower than UDP buffer size
        "MAX_SPLITTNES_RATE": 3,    # number of neighboring peers which the node take chunks of a file in parallel
        "MAX_NODE_CONNECTION":10,
        "MIN_NODE_CONNECTION":6,
        "NODE_TIME_INTERVAL": 20,        # the interval time that each node periodically informs the tracker (in seconds)
        "TRACKER_TIME_INTERVAL": 22,     # the interval time that the tracker periodically checks which nodes are in the torrent (in seconds)
        "TRACKER_IP":'localhost',         # tracker ip
        "CHUNK_SIZE":1*1024*1024,         #chunk size
        "GENERATE_FILE_SIZE": 10*1024*1024, #generate file size is 10MB
        "TEST_NODE_NUM": 20,              #the number of node which gonna test
        "TIME_UNIT_LENGTH":100,           #time unit length is 100ms
        "CHUNK_LIMIT":4,                  #chunk number for announce
        "CHUNK_PERSECOND":10
    },
    "command": {
        "CONN": 0,                  # tracker tells the node to connect the other node
        "SEND": 1,                  # traker tells the node to send file to the other node
        "NEIGHBOUR":2,              # node tells the tracker its neighbour list
        "NEED": 3,                  # node tells the tracker that it needs a file
        "LISTEN_PORT":4,            # node tells the tracker that its listen port
        "REQUEST_LINKLIST":5,       # node require the linklist from tracker
        "UPDATE": 6,                # node tells tracker that its network situation changed need to be updated
        "TORRENT": 7,               # tracker tells the node that node can start the normal torrent phase
        "RANDOM_FIFO": 8,           # tracker tells the node that node can start the scheduling phase by using random fifo algorithm
        "RANDOM_FASTEST_FAST": 9,   # tracker tells the node that node can start the scheduling phase by using random fastest fast algorithm
        "GREEDY_FASTEST_FAST": 10,  # tracker tells the node that node can start the scheduling phase by using greedy fastest fast algorithm
        "MAX_FLOW": 11,             # tracker tells the node that node can start the scheduling phase by using max flow algorithm
        "NETWORK_LIST":12,          # tracker tells the node that the whole network have how many nodes
        "NODE_OWNTABLE":13,         # node require from the other node chunk and send own table
        "CHUNK":14,                 # for chunk sending
        "FINISH_SEND":15,           # node tells the tracker that file transfer finished
        "OK_START":16,              # node tells the tracker that it is ready for start
        "NODE_REQUEST":17,          # node request from the other node
        "OWNTABLE2TRACKER":18,      # node report its own table to tracker
        "OWNTABLE_RECV":19          # tracker receive node's own table

    }
}


class Config:
    """Config class which contains directories, constants, etc."""

    def __init__(self, directory, constants, command):
        self.directory = directory
        self.constants = constants
        self.command = command

    @classmethod
    def from_json(cls, cfg):
        """Creates config from json"""
        params = json.loads(json.dumps(cfg), object_hook=HelperObject)
        return cls(params.directory, params.constants, params.command)


class HelperObject(object):
    """Helper class to convert json into Python object"""
    def __init__(self, dict_):
        self.__dict__.update(dict_)