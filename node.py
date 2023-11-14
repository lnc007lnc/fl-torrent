# built-in libraries
import copy
import threading

from utils import *
import argparse
from threading import Thread, Timer
from operator import itemgetter
import datetime
import time
from itertools import groupby
import mmap
import warnings
import numpy as np
warnings.filterwarnings("ignore")
import uuid
import gc

# implemented classes
from configs import CFG, Config
config = Config.from_json(CFG)
from messages.message import Message
from messages.node2tracker import Node2Tracker
from messages.command import Command
from messages.chunk_sharing import ChunkSharing


next_call = time.time()

class Node:
    def __init__(self, listen_port: int):
        self.listen_port = listen_port #listen connection request
        self.listen_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listen_socket.bind(('localhost', listen_port))
        self.node_addr=self.listen_socket.getsockname()
        self.node_id=None
        self.uuid=uuid.uuid4()
        self.listen_socket.listen(5)  # Listen for incoming connections, with a backlog of 5
        #self.files = self.generate_file()
        self.downloaded_files = {}
        self.threads = {}  # Dictionary to hold ConnectionThread instances
        self.receive_queue = queue.Queue()  # Single queue for received data
        self.neighbour_list=[]
        self.last_neighbour_list=[]
        self.neighbour_owntable={}
        self.neighbour_sent=[]
        self.lock = threading.Lock()
        #for test
        self.ready_flag=False
        self.network_list=[]
        self.uplink_limit=random.randint(50, 100)
        self.downlink_limit=random.randint(400, 800)
        self.own_table=None
        self.old_chunk_num=None
        self.random_data=bytes(random.getrandbits(8)for _ in range(config.constants.CHUNK_SIZE))
        #self.random_data=None
        self.count=0

        self.debug_count=0



    #####################################network function############################

    #creat node-tracker or node-node connection
    def create_connection(self, addr):
        send_queue = queue.Queue()
        conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        conn.connect(addr)
        thread = ConnectionThread(send_queue, self.receive_queue, conn, self.cleanup_callback, self.uuid,
                                  self.uplink_limit, self.downlink_limit)
        thread.start()
        thread.initialized.wait()

        thread_id = thread.id  # Create a unique id for this thread
        self.threads[thread_id] = (thread, send_queue)  # Store the thread and its send_queue

    #receive the other node connection request and initialize
    def accept_connections(self):
        while True:
            conn, addr = self.listen_socket.accept()  # Accept a new connection
            send_queue = queue.Queue()
            thread = ConnectionThread(send_queue, self.receive_queue, conn, self.cleanup_callback, self.uuid,
                                      self.uplink_limit, self.downlink_limit)
            thread.start()
            thread.initialized.wait()

            thread_id = thread.id
            self.threads[thread_id] = (thread, send_queue)

    # Start a new thread to accept connections
    def start_accepting_connections(self):
        accept_thread = threading.Thread(target=self.accept_connections)
        accept_thread.start()

    # call back to clean the link thread
    def cleanup_callback(self, thread_id):
        if thread_id in self.threads:
            _, send_queue = self.threads[thread_id]
            del self.threads[thread_id]


    def send_data(self, thread_id, data):
        # Find the thread and send_queue for the given id
        data=Message.encode(data)
        thread, send_queue = self.threads[thread_id]
        send_queue.put(data)

    def connect_tracker(self):
        if 'tracker' not in self.threads:
            tracker_send_queue = queue.Queue()
            conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            conn.connect(tuple(config.constants.TRACKER_ADDR))
            self.node_id=conn.getsockname()
            tracker_thread = ConnectionThread(tracker_send_queue, self.receive_queue,conn,self.cleanup_callback, self.uuid,
                                              self.uplink_limit, self.downlink_limit)
            self.threads['tracker'] = (tracker_thread, tracker_send_queue)
            tracker_thread.start()  # Start the thread to handle the tracker connection
            #tracker_thread.join(1)

            msg = Command(command=config.command.LISTEN_PORT, extra_information=(self.node_id[0],self.listen_port,self.uplink_limit,self.downlink_limit))
            self.send_data(thread_id='tracker', data=msg)
            msg1 = Command(command=config.command.REQUEST_LINKLIST, extra_information=self.listen_port)
            self.send_data(thread_id='tracker', data=msg1)

        log_content = f"You entered Torrent."
        log(node_id=self.uuid, content=log_content)

    #############################################Torrent function##############################
    # Generate a file for testing
    def generate_file(self):
        files = []
        node_files_dir = config.directory.node_files_dir + 'node' + str(self.node_id)
        if os.path.isdir(node_files_dir):
            _, _, files = next(os.walk(node_files_dir))
        else:
            os.makedirs(node_files_dir)
            with open(node_files_dir+"/test", 'wb') as f:
                data = os.urandom(config.constants.GENERATE_FILE_SIZE)
                f.write(data)
            _, _, files = next(os.walk(node_files_dir))
        return files

    def split_file_to_chunks(self, file_path: str, rng: tuple, node_id: tuple) -> list:
        with open(file_path, "r+b") as f:
            mm = mmap.mmap(f.fileno(), 0)[rng[0]: rng[1]]
            # we divide each chunk to a fixed-size pieces to be transferable
            piece_size = config.constants.CHUNK_PIECES_SIZE
            # return chunk with id, node_id+chunk_index=chunk_id
            return [(node_id + (index,), mm[p: p + piece_size]) for index, p in
                    enumerate(range(0, rng[1] - rng[0], piece_size), start=1)]

    def reassemble_file(self, chunks: list, file_path: str):
        with open(file_path, "wb+") as f:
            # Sort the chunks by the node_id and index before writing them
            chunks.sort(key=lambda x: x[0])  # Assuming x[0] is the tuple (node_id, chunk_index)
            for chunk_info, chunk_data in chunks:
                f.write(chunk_data)

    def send_chunk(self, filename: str, rng: tuple, dest_node_id):
        file_path = f"{config.directory.node_files_dir}node{self.node_id}/{filename}"
        chunk_pieces = self.split_file_to_chunks(file_path=file_path, rng=rng)

        for idx, p in enumerate(chunk_pieces):
            msg = ChunkSharing(src_node_id=self.node_id,
                               dest_node_id=dest_node_id,
                               filename=filename,
                               range=rng,
                               idx=idx,
                               chunk=p)
            log_content = f"The {idx}/{len(chunk_pieces)} has been sent!"
            log(node_id=self.node_id, content=log_content)
            self.send_data(thread_id=dest_node_id, data=Message.encode(msg))

        # now let's tell the neighboring peer that sending has finished (idx = -1)
        msg = ChunkSharing(src_node_id=self.node_id,
                           dest_node_id=dest_node_id,
                           filename=filename,
                           range=rng)
        self.send_data(thread_id=dest_node_id, data=Message.encode(msg))

        log_content = "The process of sending a chunk to node{} of file {} has finished!".format(dest_node_id,
                                                                                                 filename)
        log(node_id=self.node_id, content=log_content)

        msg = Node2Tracker(node_id=self.node_id,
                           filename=filename)
        self.send_data(thread_id=dest_node_id, data=Message.encode(msg))

    def sort_downloaded_chunks(self, filename: str) -> list:
        sort_result_by_range = sorted(self.downloaded_files[filename],
                                      key=itemgetter("range"))
        group_by_range = groupby(sort_result_by_range,
                                 key=lambda i: i["range"])
        sorted_downloaded_chunks = []
        for key, value in group_by_range:
            value_sorted_by_idx = sorted(list(value),
                                         key=itemgetter("idx"))
            sorted_downloaded_chunks.append(value_sorted_by_idx)

        return sorted_downloaded_chunks

    #maynot use
    def split_file_owners(self, file_owners: list, filename: str):
        owners = []
        for owner in file_owners:
            if owner[0]['node_id'] != self.node_id:
                owners.append(owner)
        if len(owners) == 0:
            log_content = f"No one has {filename}"
            log(node_id=self.node_id, content=log_content)
            return
        # sort owners based on their sending frequency
        owners = sorted(owners, key=lambda x: x[1], reverse=True)

        to_be_used_owners = owners[:config.constants.MAX_SPLITTNES_RATE]
        # 1. first ask the size of the file from peers
        log_content = f"You are going to download {filename} from Node(s) {[o[0]['node_id'] for o in to_be_used_owners]}"
        log(node_id=self.node_id, content=log_content)
        file_size = self.ask_file_size(filename=filename, file_owner=to_be_used_owners[0])
        log_content = f"The file {filename} which you are about to download, has size of {file_size} bytes"
        log(node_id=self.node_id, content=log_content)

        # 2. Now, we know the size, let's split it equally among peers to download chunks of it from them
        step = file_size / len(to_be_used_owners)
        chunks_ranges = [(round(step*i), round(step*(i+1))) for i in range(len(to_be_used_owners))]

        # 3. Create a thread for each neighbor peer to get a chunk from it
        self.downloaded_files[filename] = []
        neighboring_peers_threads = []
        for idx, obj in enumerate(to_be_used_owners):
            t = Thread(target=self.receive_chunk, args=(filename, chunks_ranges[idx], obj))
            #t.setDaemon(True)
            t.start()
            neighboring_peers_threads.append(t)
        for t in neighboring_peers_threads:
            t.join()

        log_content = "All the chunks of {} has downloaded from neighboring peers. But they must be reassembled!".format(filename)
        log(node_id=self.node_id, content=log_content)

        # 4. Now we have downloaded all the chunks of the file. It's time to sort them.
        sorted_chunks = self.sort_downloaded_chunks(filename=filename)
        log_content = f"All the pieces of the {filename} is now sorted and ready to be reassembled."
        log(node_id=self.node_id, content=log_content)

        # 5. Finally, we assemble the chunks to re-build the file
        total_file = []
        file_path = f"{config.directory.node_files_dir}node{self.node_id}/{filename}"
        for chunk in sorted_chunks:
            for piece in chunk:
                total_file.append(piece["chunk"])
        self.reassemble_file(chunks=total_file,
                             file_path=file_path)
        log_content = f"{filename} has successfully downloaded and saved in my files directory."
        log(node_id=self.node_id, content=log_content)
        self.files.append(filename)

    def search_torrent(self, filename: str) -> dict:
        msg = Node2Tracker(node_id=self.node_id,
                           filename=filename)
        search_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        search_sock.connect(tuple(config.constants.TRACKER_ADDR))
        search_sock.send(msg.encode())
        data = search_sock.recv(config.constants.BUFFER_SIZE)
        tracker_msg = Message.decode(data)
        search_sock.close()
        return tracker_msg


    def fetch_owned_files(self) -> list:
        files = []
        node_files_dir = config.directory.node_files_dir + 'node' + str(self.node_id)
        if os.path.isdir(node_files_dir):
            _, _, files = next(os.walk(node_files_dir))
        else:
            os.makedirs(node_files_dir)
        return files

    ##############################################process command###############################
    def process_command(self, data,thread_id):
        print('command')
        command=data['command']
        if command==config.command.CONN:
            linklist=data['extra_information']
            for thread_id in linklist:
                self.create_connection(addr=thread_id)
        elif command==config.command.NETWORK_LIST:
            self.network_list=data['extra_information']
            index_own = self.network_list.index(str(self.uuid))
            length = int(config.constants.GENERATE_FILE_SIZE / config.constants.CHUNK_SIZE)
            width = len(self.network_list)
            self.own_table = np.zeros((length, width))  # Create a 2D numpy array of zeros with the correct shape
            self.own_table[:, index_own] = 1  # Set all values in the column corresponding to `self.node_id` to 1
            self.old_chunk_num=self.own_table.sum()
        elif command==config.command.NODE_OWNTABLE and self.ready_flag:
            print('OWNTABLE')
            with self.lock:
                print(data['extra_information'])
                self.neighbour_owntable[thread_id]=data['extra_information']
                print(self.neighbour_owntable.keys())
        elif command==config.command.NODE_REQUEST:
            print('REQUEST')
            matching_coordinates=data['extra_information']
            for i,j in matching_coordinates:
                command_send = Command(command=config.command.CHUNK, extra_information=(i, j, self.random_data))
                self.send_data(thread_id=thread_id, data=command_send)
        elif command==config.command.CHUNK:
            print('CHUNK')
            i,j,_=data['extra_information']
            print((i,j))
            flag=data['flag']
            self.own_table[i,j]=1

            if thread_id in self.neighbour_sent:
                self.neighbour_sent.remove(thread_id)

            if flag==config.command.RANDOM_FIFO:
                now_chunk_num=self.own_table.sum()
                if now_chunk_num-self.old_chunk_num>=config.constants.MIN_CHUNK_LIMIT:
                    command_send = Command(command=config.command.OK_TORRENT, extra_information=None)
                    self.send_data(thread_id='tracker', data=command_send)
                elif self.neighbour_sent==[]: #keep asking the tracker
                    command_send = Command(command=config.command.OWNTABLE_RECV, extra_information=self.own_table,
                                           flag=config.command.RANDOM_FIFO)
                    self.send_data('tracker', command_send)
                print(self.own_table)
            elif flag==config.command.RANDOM_FASTEST_FAST:
                now_chunk_num = self.own_table.sum()
                if now_chunk_num - self.old_chunk_num >= config.constants.MIN_CHUNK_LIMIT:
                    command_send = Command(command=config.command.OK_TORRENT, extra_information=None)
                    self.send_data(thread_id='tracker', data=command_send)
                elif self.neighbour_sent == []:  # keep asking the tracker
                    command_send = Command(command=config.command.OWNTABLE_RECV, extra_information=self.own_table,
                                           flag=config.command.RANDOM_FASTEST_FAST)
                    self.send_data('tracker', command_send)
                print(self.own_table)
            elif flag==config.command.GREEDY_FASTEST_FAST:
                now_chunk_num = self.own_table.sum()
                if now_chunk_num - self.old_chunk_num >= config.constants.MIN_CHUNK_LIMIT:
                    command_send = Command(command=config.command.OK_TORRENT, extra_information=None)
                    self.send_data(thread_id='tracker', data=command_send)
                elif self.neighbour_sent == []:  # keep asking the tracker
                    command_send = Command(command=config.command.OWNTABLE_RECV, extra_information=self.own_table,
                                           flag=config.command.GREEDY_FASTEST_FAST)
                    self.send_data('tracker', command_send)
                print(self.own_table)
        elif command==config.command.SEND:
            matching_coordinates = data['extra_information']
            flag=data['flag']
            recv=data['recv']
            for i,j in matching_coordinates:
                command_send = Command(command=config.command.CHUNK, extra_information=(i, j, self.random_data),
                                       flag=flag)
                self.send_data(thread_id=recv, data=command_send)
        elif command==config.command.OWNTABLE2TRACKER:
            print('config.command.OWNTABLE2TRACKER')
            flag=data['flag']
            command_send=Command(command=config.command.OWNTABLE_RECV, extra_information=self.own_table, flag=flag)
            self.send_data('tracker',command_send)

        elif command==config.command.TORRENT:
            print('TORRENT')
            self.ready_flag=True
            data = Command(command=config.command.NODE_OWNTABLE, extra_information=self.own_table)
            for thread_id in self.neighbour_list:
                self.send_data(thread_id=thread_id, data=data)
            thread = threading.Thread(target=self.torrent)
            thread.start()
        elif command==config.command.CLEAR_ALL:
            self.ready_flag = False

            index_own = self.network_list.index(str(self.uuid))
            length = int(config.constants.GENERATE_FILE_SIZE / config.constants.CHUNK_SIZE)
            width = len(self.network_list)
            self.own_table = np.zeros((length, width))  # Create a 2D numpy array of zeros with the correct shape
            self.own_table[:, index_own] = 1  # Set all values in the column corresponding to `self.node_id` to 1
            self.old_chunk_num = self.own_table.sum()

            self.count = 0
            self.debug_count = 0
            # clear queue
            while not self.receive_queue.empty():
                self.receive_queue.get()
                self.receive_queue.task_done()
            print(self.own_table)

    def torrent(self):
        temp_table=None
        while True:
            if not np.array_equal(temp_table,self.own_table):
                print('Send own table')
                temp_table=self.own_table.copy()
                data = Command(command=config.command.NODE_OWNTABLE, extra_information=self.own_table)
                for thead_id in self.neighbour_list:
                    self.send_data(thread_id=thead_id, data=data)
            if np.all(self.own_table == 1):
                data = Command(command=config.command.FINISH_SEND, extra_information=time.time())
                self.send_data(thread_id='tracker', data=data)
                break
            if bool(self.neighbour_owntable):
                with self.lock:
                    for thread_id in self.neighbour_owntable.keys():
                        require_table = 1 - self.own_table
                        print('own_table')
                        print(self.own_table)
                        both_one = np.logical_and(self.neighbour_owntable[thread_id], require_table)
                        coordinates = np.where(both_one)
                        matching_coordinates = list(zip(coordinates[0], coordinates[1]))
                        # k = min(config.constants.MAGIC_FIFO_NUM, len(matching_coordinates))
                        # random_selection = random.sample(matching_coordinates, k)
                        command_send = Command(command=config.command.NODE_REQUEST, extra_information=matching_coordinates)
                        self.send_data(thread_id=thread_id, data=command_send)
                time.sleep(5)
        exit()



        # print(self.own_table)
        # while True:
        #     data = Command(command=config.command.NODE_OWNTABLE, extra_information=self.own_table)
        #     for thead_id in self.neighbour_list:
        #         self.send_data(thread_id=thead_id, data=data)
        #     if np.all(self.own_table == 1):
        #         data = Command(command=config.command.FINISH_SEND, extra_information=None)
        #         self.send_data(thread_id='tracker', data=data)
        #         break
        #
        #     time.sleep(0.01)  # every 100ms check
    ##############################################run##############################################
    def run(self):
        log_content = f"***************** Node program started just right now! *****************"
        log(node_id=self.node_id, content=log_content)
        self.connect_tracker()
        self.start_accepting_connections()


        # receive command from the other
        while True:
            gc.collect()
            try:
                # monitor the change of neighbour list
                self.neighbour_list = list(self.threads.keys())
                self.neighbour_list.remove('tracker')
                self.neighbour_sent=self.neighbour_list.copy()
                if set(self.last_neighbour_list) != set(self.neighbour_list):
                    neighbour_command = Command(command=config.command.NEIGHBOUR, extra_information=self.neighbour_list)
                    self.send_data(thread_id='tracker', data=neighbour_command)  # tells the tracker node's neighbour
                    self.last_neighbour_list = self.neighbour_list.copy()
                    self.neighbour_sent = self.neighbour_list.copy()

                thread_id, received_data = self.receive_queue.get_nowait()
                # Process the received data
                received_data = Message.decode(received_data)
                self.process_command(received_data,thread_id)
            except queue.Empty:
                continue  # No data received, continue to the next iteration

        # input command manually for debugging
        # print("ENTER YOUR COMMAND!")
        # while True:
        #     command = input()
        #     mode, filename = parse_command(command)
        #
        #     #################### send mode ####################
        #     if mode == 'send':
        #         self.set_send_mode(filename=filename)
        #     #################### download mode ####################
        #     elif mode == 'download':
        #         t = Thread(target=node.set_download_mode, args=(filename,))
        #         #t.setDaemon(True)
        #         t.start()
        #     #################### exit mode ####################
        #     elif mode == 'exit':
        #         self.exit_torrent()
        #         exit(0)


if __name__ == '__main__':
    node = Node(listen_port=generate_random_port())
    # run the node
    node.run()
