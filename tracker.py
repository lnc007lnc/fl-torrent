# built-in libraries
from threading import Thread, Timer
from collections import defaultdict
import json
import datetime
import time
import warnings
import zmq
warnings.filterwarnings("ignore")

# implemented classes
from utils import *
from messages.message import  Message
from messages.tracker2node import Tracker2Node
from messages.command import Command
from segment import UDPSegment
from configs import CFG, Config
config = Config.from_json(CFG)

next_call = time.time()



class Tracker:
    def __init__(self):
        self.listen_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listen_socket.bind(tuple(config.constants.TRACKER_ADDR))
        self.listen_socket.listen(5)  # Listen for incoming connections, with a backlog of 5
        self.file_owners_list = defaultdict(list)
        self.send_freq_list = defaultdict(int)
        self.neighbour_list = defaultdict(list)
        self.nodedict= {}
        self.has_informed_tracker = defaultdict(bool)
        self.threads = {}  # Dictionary to hold ConnectionThread instances
        self.receive_queue = queue.Queue()  # Single queue for received data


    #####################################network function############################
    def send_data(self, thread_id, data):
        # Find the thread and send_queue for the given id
        thread, send_queue = self.threads[thread_id]
        data=Message.encode(data)
        send_queue.put(data)

    def accept_connections(self):
        while True:
            conn, addr = self.listen_socket.accept()  # Accept a new connection
            thread_id = addr  # Create a unique id for this thread
            send_queue = queue.Queue()
            thread = ConnectionThread(thread_id, send_queue, self.receive_queue, conn, self.cleanup_callback)
            self.threads[thread_id] = (thread, send_queue)
            thread.start()


    # Start a new thread to accept connections
    def start_accepting_connections(self):
        accept_thread = threading.Thread(target=self.accept_connections)
        accept_thread.start()

    #call back to clean the link thread
    def cleanup_callback(self, thread_id):
        if thread_id in self.threads:
            _, send_queue = self.threads[thread_id]
            del self.threads[thread_id]
            del self.nodedict[thread_id]

    #randomly choose link list for new node
    def return_linklist(self,node_id,num):
        templist=list(self.nodedict.keys())
        print(templist)
        if node_id in templist:
            templist.remove(node_id)
        random_node_list=random.sample(templist,min(num,len(templist)))
        return [self.nodedict[k] for k in random_node_list]



    #############################################Torrent function##############################
    def add_file_owner(self, msg: dict, addr: tuple):
        entry = {
            'node_id': msg['node_id'],
            'addr': addr
        }
        log_content = f"Node {msg['node_id']} owns {msg['filename']} and is ready to send."
        log(node_id=0, content=log_content, is_tracker=True)

        self.file_owners_list[msg['filename']].append(json.dumps(entry))
        self.file_owners_list[msg['filename']] = list(set(self.file_owners_list[msg['filename']]))
        self.send_freq_list[msg['node_id']] += 1
        self.send_freq_list[msg['node_id']] -= 1

        self.save_db_as_json()

    def update_db(self, msg: dict):
        self.send_freq_list[msg["node_id"]] += 1
        self.save_db_as_json()


    def search_file(self, msg: dict, addr: tuple):
        log_content = f"Node{msg['node_id']} is searching for {msg['filename']}"
        log(node_id=0, content=log_content, is_tracker=True)

        matched_entries = []
        for json_entry in self.file_owners_list[msg['filename']]:
            entry = json.loads(json_entry)
            matched_entries.append((entry, self.send_freq_list[entry['node_id']]))

        tracker_response = Tracker2Node(dest_node_id=msg['node_id'],
                                        search_result=matched_entries,
                                        filename=msg['filename'])

        self.send_data(thread_id=msg['node_id'],data=tracker_response)

    def save_db_as_json(self):
        if not os.path.exists(config.directory.tracker_db_dir):
            os.makedirs(config.directory.tracker_db_dir)

        nodes_info_path = config.directory.tracker_db_dir + "nodes.json"
        files_info_path = config.directory.tracker_db_dir + "files.json"

        # saves nodes' information as a json file
        temp_dict = {}
        for key, value in self.send_freq_list.items():
            temp_dict['node'+str(key)] = value
        nodes_json = open(nodes_info_path, 'w')
        json.dump(temp_dict, nodes_json, indent=4, sort_keys=True)

        # saves files' information as a json file
        files_json = open(files_info_path, 'w')
        json.dump(self.file_owners_list, files_json, indent=4, sort_keys=True)

    def handle_node_request(self, data: bytes, addr: tuple):
        msg = Message.decode(data)
        mode = msg['mode']
        if mode == config.tracker_requests_mode.OWN:
            self.add_file_owner(msg=msg, addr=addr)
        elif mode == config.tracker_requests_mode.NEED:
            self.search_file(msg=msg, addr=addr)
        elif mode == config.tracker_requests_mode.UPDATE:
            self.update_db(msg=msg)
        elif mode == config.tracker_requests_mode.REGISTER:
            self.has_informed_tracker[(msg['node_id'], addr)] = True

    ##############################################process command###############################
    def process_command(self, data, thread_id):
        print("command")
        print("command")
        print("command")
        command=data['command']
        if command==config.command.NEIGHBOUR:
            print("config.command.NEIGHBOUR")
            print("config.command.NEIGHBOUR")
            print("config.command.NEIGHBOUR")
            neighbour_list=data['extra_information']
            self.neighbour_list[thread_id]=neighbour_list
            for key in neighbour_list:
                print(key+":"+self.neighbour_list[key])
        elif command==config.command.LISTEN_PORT:
            print("config.command.LISTEN_PORT")
            print("config.command.LISTEN_PORT")
            print("config.command.LISTEN_PORT")
            listen_port=data['extra_information']
            self.nodedict[thread_id]=(thread_id[0],listen_port)
            print(self.nodedict.keys())
        elif command==config.command.REQUEST_LINKLIST:
            print("config.command.REQUEST_LINKLIST")
            print("config.command.REQUEST_LINKLIST")
            print("config.command.REQUEST_LINKLIST")

            linklist=self.return_linklist(thread_id,config.constants.MIN_NODE_CONNECTION)
            command=Command(command=config.command.CONN,extra_information=linklist)
            self.send_data(thread_id,command)



    def run(self):
        log_content = f"***************** Tracker program started just right now! *****************"
        log(node_id=0, content=log_content, is_tracker=True)
        self.start_accepting_connections()
        #self.accept_connections()

        # receive command from the other
        while True:
            try:
                thread_id, received_data = self.receive_queue.get_nowait()
            except queue.Empty:
                continue  # No data received, continue to the next iteration
            # Process the received data
            received_data = Message.decode(received_data)
            print(received_data)
            self.process_command(received_data, thread_id)

if __name__ == '__main__':
    t = Tracker()
    t.run()