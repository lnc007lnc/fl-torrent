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
        "TRACKER_ADDR": ('localhost', 12345),
        #"TRACKER_ADDR": ('0.0.0.0', 12345),
        "MAX_UDP_SEGMENT_DATA_SIZE": 65527,
        "BUFFER_SIZE": 9216,        # MACOSX UDP MTU is 9216
        "CHUNK_PIECES_SIZE": 9216 - 2000, # Each chunk pieces(segments of UDP) must be lower than UDP buffer size
        "MAX_SPLITTNES_RATE": 3,    # number of neighboring peers which the node take chunks of a file in parallel
        "MAX_NODE_CONNECTION":10,
        "MIN_NODE_CONNECTION":3,
        "NODE_TIME_INTERVAL": 20,        # the interval time that each node periodically informs the tracker (in seconds)
        "TRACKER_TIME_INTERVAL": 22,      #the interval time that the tracker periodically checks which nodes are in the torrent (in seconds)
        "TRACKER_IP":'localhost'          #tracker ip
    },
    "command": {
        "CONN": 0,      # tracker tells the node to connect the other node
        "SEND": 1,      # traker tells the node to send file to the other node
        "NEIGHBOUR":2,  # node tells the tracker its neighbour list
        "NEED": 3,      # node tells the tracker that it needs a file
        "LISTEN_PORT":4,# node tells the tracker that its listen port
        "REQUEST_LINKLIST":5, #node require the linklist from tracker
        "UPDATE": 6     # node tells tracker that its network situation changed need to be updated
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