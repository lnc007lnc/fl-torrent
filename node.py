# built-in libraries
from utils import *
import argparse
from threading import Thread, Timer
from operator import itemgetter
import datetime
import time
from itertools import groupby
import mmap
import warnings
warnings.filterwarnings("ignore")

# implemented classes
from configs import CFG, Config
config = Config.from_json(CFG)
from messages.message import Message
from messages.node2tracker import Node2Tracker
from messages.node2node import Node2Node
from messages.chunk_sharing import ChunkSharing
from segment import UDPSegment

import uuid

next_call = time.time()

class Node:
    def __init__(self, listen_port: int):
        self.listen_port = listen_port #listen connection request
        self.listen_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listen_socket.bind(('0.0.0.0', listen_port))
        self.node_addr=self.listen_socket.getsockname()
        self.node_id=self.node_addr
        self.listen_socket.listen(5)  # Listen for incoming connections, with a backlog of 5
        self.files = self.fetch_owned_files()
        self.downloaded_files = {}
        self.threads = {}  # Dictionary to hold ConnectionThread instances
        self.receive_queue = queue.Queue()  # Single queue for received data

    #####################################network function############################

    #creat node-tracker or node-node connection
    def create_connection(self, addr):
        send_queue = queue.Queue()
        thread_id = f'{addr[0]}:{addr[1]}'  # Create a unique id for this thread
        conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        conn.connect(addr)
        thread = ConnectionThread(thread_id, send_queue, self.receive_queue, conn)
        self.threads[thread_id] = (thread, send_queue)  # Store the thread and its send_queue
        thread.start()

    #receive the other node connection request and initialize
    def accept_connections(self):
        while True:
            conn, addr = self.listen_socket.accept()  # Accept a new connection
            thread_id = f'{addr[0]}:{addr[1]}'  # Create a unique id for this thread
            send_queue = queue.Queue()
            thread = ConnectionThread(thread_id, send_queue, self.receive_queue, conn)
            self.threads[thread_id] = (thread, send_queue)
            thread.start()

    # Start a new thread to accept connections
    def start_accepting_connections(self):
        accept_thread = threading.Thread(target=self.accept_connections)
        accept_thread.start()

    def send_data(self, thread_id, data):
        # Find the thread and send_queue for the given id
        thread, send_queue = self.threads[thread_id]
        send_queue.put(data)

    def connect_tracker(self):
        tracker_id = 'tracker'
        if tracker_id not in self.threads:
            tracker_send_queue = queue.Queue()
            conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            conn.connect(tuple(config.constants.TRACKER_ADDR))
            tracker_thread = ConnectionThread(tracker_id, tracker_send_queue, self.receive_queue,conn)
            self.threads[tracker_id] = (tracker_thread, tracker_send_queue)
            tracker_thread.start()  # Start the thread to handle the tracker connection

        msg = Node2Tracker(node_id=self.node_id, filename="")
        self.send_data(thread_id='tracker',data=Message.encode(msg))

        log_content = f"You entered Torrent."
        log(node_id=self.node_id, content=log_content)

    #############################################Torrent function##############################
    def split_file_to_chunks(self, file_path: str, rng: tuple) -> list:
        with open(file_path, "r+b") as f:
            mm = mmap.mmap(f.fileno(), 0)[rng[0]: rng[1]]
            # we divide each chunk to a fixed-size pieces to be transferable
            piece_size = config.constants.CHUNK_PIECES_SIZE
            return [mm[p: p + piece_size] for p in range(0, rng[1] - rng[0], piece_size)]

    def reassemble_file(self, chunks: list, file_path: str):
        with open(file_path, "bw+") as f:
            for ch in chunks:
                f.write(ch)
            f.flush()
            f.close()

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
    def process_command(self, command):
        pass

    ##############################################run##############################################
    def run(self):
        log_content = f"***************** Node program started just right now! *****************"
        log(node_id=self.node_id, content=log_content)
        self.connect_tracker()
        self.start_accepting_connections()

        # receive command from the other
        while True:
            try:
                thread_id, received_data = self.receive_queue.get_nowait()
            except queue.Empty:
                continue  # No data received, continue to the next iteration
            # Process the received data
            self.process_command(received_data)

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
