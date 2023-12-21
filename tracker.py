# built-in libraries
from threading import Thread, Timer
from collections import defaultdict
import json
import datetime
import time
import warnings
warnings.filterwarnings("ignore")

# implemented classes
from utils import *
from messages.message import Message
from messages.tracker2node import Tracker2Node
from messages.command import Command
from segment import UDPSegment
from configs import CFG, Config
config = Config.from_json(CFG)
import numpy as np
import matplotlib.pyplot as plt
from NetworkFlowModel import NetworkFlowModel
from Random_FIFO import Random_FIFO
from Random_FastestFirst import Random_FastestFirst
from Greedy_FastestFirst import Greedy_FastestFirst
from Max_Flow import Max_Flow



class Tracker:
    def __init__(self):
        self.listen_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listen_socket.bind(tuple(config.constants.TRACKER_ADDR))
        self.listen_socket.listen(5)  # Listen for incoming connections, with a backlog of 5
        self.file_owners_list = defaultdict(list)
        self.send_freq_list = defaultdict(int)
        self.neighbour_list = defaultdict(list)
        self.nodedict= {}
        self.node_limit={}
        self.has_informed_tracker = defaultdict(bool)
        self.threads = {}  # Dictionary to hold ConnectionThread instances
        self.receive_queue = queue.Queue()  # Single queue for received data
        self.node_owntable={}
        self.node_owntable_old={}
        self.own_table_num=0
        self.uuid='tracker'
        self.uplink_limit=100000
        self.downlink_limit=100000
        #for test
        self.ok_torrent = 0
        self.ok_torrent_set=set()
        self.ok_torrent_time = []
        self.finish_program_num=0 #for detecting node's file transfer ending
        self.finish_time=0
        self.input_flag=True #for input command manualy
        self.start_time=0
        self.debug_count=0
        self.finish_up_node_num=0
        self.finish_down_node_num=0
        self.time_slot=0
        self.algo_flag=None
        self.send_num=0
        self.send_count=-1
        self.increments = {}
        self.bw_utilization={}
        self.bw_utilization_time = {}
        self.bw_max=0
        self.algo_result_dict={}




    #####################################network function############################
    def send_data(self, thread_id, data):
        # Find the thread and send_queue for the given id
        thread, send_queue = self.threads[thread_id]
        data=Message.encode(data)
        send_queue.put(data)

    #send data to all nodes
    def send_data_all(self, data):
        for thread_id in self.threads.keys():
            self.send_data(thread_id,data)

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

    #call back to clean the link thread
    def cleanup_callback(self, thread_id):
        if thread_id in self.threads:
            _, send_queue = self.threads[thread_id]
            del self.threads[thread_id]
            del self.nodedict[thread_id]
            del self.neighbour_list[thread_id]

    #randomly choose link list for new node
    def return_linklist(self,node_id,num):
        templist=list(self.nodedict.keys())
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
        command=data['command']
        if command==config.command.UPONESLOT:
            temp_time_slot = data['extra_information']
            if temp_time_slot == self.time_slot:
                self.finish_up_node_num+=1
                print('UPONESLOT : '+str(self.finish_up_node_num))
        elif command==config.command.DOWNONESLOT:
            temp_time_slot=data['extra_information']
            if temp_time_slot==self.time_slot:
                self.finish_down_node_num+=1
                print('DOWNONESLOT : '+str(self.finish_down_node_num))
        elif command==config.command.ALGO_SEND:
            if self.send_count==-1:
                self.send_count=1
            else:
                self.send_count+=1
        elif command==config.command.NEIGHBOUR:
            print("config.command.NEIGHBOUR")
            neighbour_list=data['extra_information']
            self.neighbour_list[thread_id]=neighbour_list
            # for key in self.neighbour_list:
            #     print(key[0] + ":" + str(key[1]) + "  :" + ''.join(str(item) for item in self.neighbour_list[key]))
        elif command==config.command.LISTEN_PORT:
            ip,port,uplink,downlink=data['extra_information']
            print(data['extra_information'])
            self.nodedict[thread_id]=(ip,port)
            self.node_limit[thread_id]=(uplink,downlink)
        elif command==config.command.OK_TORRENT:
            if thread_id not in self.ok_torrent_set:
                self.ok_torrent_set.add(thread_id)
                self.ok_torrent+=1
                self.ok_torrent_time.append(time.time() - self.start_time +self.time_slot*config.constants.TIME_UNIT_LENGTH)
            # if self.ok_torrent<=config.constants.TEST_NODE_NUM:
            #     self.ok_torrent_time.append(time.time()-self.start_time)
            #print(f'ok_torrent: {self.ok_torrent}')
        elif command==config.command.REQUEST_LINKLIST:
            linklist=self.return_linklist(thread_id,config.constants.MIN_NODE_CONNECTION)
            command=Command(command=config.command.CONN,extra_information=linklist)
            self.send_data(thread_id,command)
        elif command==config.command.FINISH_SEND:
            print("Finish one!")
            send_time=data['extra_information']
            self.finish_time=max(self.finish_time,send_time-self.start_time)
            self.finish_program_num+=1
        elif command==config.command.OWNTABLE_RECV:
            self.node_owntable[thread_id]=data['extra_information']
            flag=data['flag']
            self.own_table_num += 1
            # if flag == config.command.RANDOM_FIFO:
            #     if self.own_table_num == config.constants.TEST_NODE_NUM:  # first time
            #         self.node_owntable_old = self.node_owntable.copy()
            #         for loop_thread_id in self.threads.keys():
            #             self.random_fifo(thread_id=loop_thread_id)
            #     elif self.own_table_num > config.constants.TEST_NODE_NUM:
            #         self.random_fifo(thread_id=thread_id)
            if flag == config.command.RANDOM_FIFO:
                if self.own_table_num == config.constants.TEST_NODE_NUM:  # first time
                    self.random_fifo(round=1)
                    self.algo_send(round=1)
            elif flag==config.command.RANDOM_FASTEST_FAST:
                if self.own_table_num == config.constants.TEST_NODE_NUM:  # first time
                    self.random_fastest_fast(round=1)
                    self.algo_send(round=1)
            elif flag == config.command.GREEDY_FASTEST_FAST:
                if self.own_table_num == config.constants.TEST_NODE_NUM:  # first time
                    self.greedy_fastest_fast(round=1)
                    self.algo_send(round=1)
            elif flag==config.command.MAX_FLOW:
                if self.own_table_num == config.constants.TEST_NODE_NUM:  # first time
                    self.max_flow(round=1)
                    self.algo_send(round=1)
            # elif flag==config.command.RANDOM_FASTEST_FAST:
            #     if self.own_table_num == config.constants.TEST_NODE_NUM:  # first time
            #         self.node_owntable_old = self.node_owntable.copy()
            #         for loop_thread_id in self.threads.keys():
            #             self.random_fastest_fast(thread_id=loop_thread_id)
            #     elif self.own_table_num > config.constants.TEST_NODE_NUM:
            #         self.random_fastest_fast(thread_id=thread_id)
            # elif flag==config.command.GREEDY_FASTEST_FAST:
            #     if self.own_table_num == config.constants.TEST_NODE_NUM:  # first time
            #         self.node_owntable_old = self.node_owntable.copy()
            #         for loop_thread_id in self.threads.keys():
            #             self.greedy_fastest_fast(thread_id=loop_thread_id)
            #     elif self.own_table_num > config.constants.TEST_NODE_NUM:
            #         self.greedy_fastest_fast(thread_id=thread_id)


    def algo_send(self, round):

        if round not in self.algo_result_dict:
            command_send = Command(command=None, extra_information=None)
            self.send_data_all(data=command_send)
            return
        send_list=self.algo_result_dict[round]
        node_list = list(self.threads.keys())
        for recv,send,chunk in send_list:
            recv_id=node_list[recv]
            send_id=node_list[send]
            i = int(chunk) % int(config.constants.GENERATE_FILE_SIZE / config.constants.CHUNK_SIZE) # chunk index
            j = int(chunk) // int(config.constants.GENERATE_FILE_SIZE / config.constants.CHUNK_SIZE)  # node index
            command_send = Command(command=config.command.SEND, extra_information=[(i,j)],
                                   flag=config.command.MAX_FLOW, recv=recv_id)
            self.send_data(thread_id=send_id, data=command_send)
            sleep_time=random.uniform(config.constants.COMMUNICATION_LAG,config.constants.COMMUNICATION_LAG*config.constants.RANDOM_LAG_TIMES)
            time.sleep(sleep_time)
            self.send_num += 1
            print((recv, send, i, j,self.send_num))
        if self.algo_flag in self.bw_utilization_time:
            self.bw_utilization_time[self.algo_flag].append(time.time() - self.start_time)
        else:
            self.bw_utilization_time[self.algo_flag] = [time.time() - self.start_time]
        print(round)
        print(len(self.bw_utilization_time[self.algo_flag]))

    #start normal torrent
    def torrent(self):
        data = Command(command=config.command.TORRENT, extra_information=None)
        self.send_data_all(data=data)

    # # scheduling algorithm1: Heuristics Random-FIFO
    # def random_fifo(self,thread_id=None):
    #     if thread_id!=None:
    #         neighbour_list = self.neighbour_list[thread_id]
    #         require_table = 1 - self.node_owntable[thread_id]
    #         for neighbour in neighbour_list:
    #             neighbour_has_table = self.node_owntable[neighbour]
    #             both_one = np.logical_and(neighbour_has_table, require_table)
    #             coordinates = np.where(both_one)
    #             matching_coordinates = list(zip(coordinates[0], coordinates[1]))
    #             k = min(config.constants.MAGIC_FIFO_NUM, len(matching_coordinates))
    #             random_selection = random.sample(matching_coordinates, k)
    #             command_send = Command(command=config.command.SEND, extra_information=random_selection,
    #                                    flag=config.command.RANDOM_FIFO, recv=thread_id)
    #             self.send_data(thread_id=neighbour, data=command_send)
    #             self.send_num+=1
    #     else:
    #         data = Command(command=config.command.OWNTABLE2TRACKER,
    #                        extra_information=None, flag=config.command.RANDOM_FIFO)
    #         self.send_data_all(data=data)
    #
    # # scheduling algorithm2: Heuristics Random-Fastest-Fast
    # def random_fastest_fast(self, thread_id=None):
    #     if thread_id!=None:
    #         neighbour_list = self.neighbour_list[thread_id]
    #         require_table = 1 - self.node_owntable[thread_id]
    #
    #         k = min(config.constants.MAGIC_FIFO_NUM, len(neighbour_list))
    #         top_k_neighbours = sorted(neighbour_list, key=lambda node: self.node_limit[node][0], reverse=True)[:k]
    #
    #         for neighbour in top_k_neighbours:
    #             neighbour_has_table = self.node_owntable[neighbour]
    #             both_one = np.logical_and(neighbour_has_table, require_table)
    #             coordinates = np.where(both_one)
    #             matching_coordinates = list(zip(coordinates[0], coordinates[1]))
    #             command_send = Command(command=config.command.SEND, extra_information=matching_coordinates,
    #                                    flag=config.command.RANDOM_FASTEST_FAST, recv=thread_id)
    #             self.send_data(thread_id=neighbour, data=command_send)
    #             self.send_num+=1
    #     else:
    #         data = Command(command=config.command.OWNTABLE2TRACKER,
    #                        extra_information=None, flag=config.command.RANDOM_FASTEST_FAST)
    #         self.send_data_all(data=data)
    #
    # # scheduling algorithm3: Heuristics Greedy-Fastest-Fast
    # def greedy_fastest_fast(self, thread_id=None):
    #     if thread_id!=None:
    #         neighbour_list = self.neighbour_list[thread_id]
    #         require_table = 1 - self.node_owntable[thread_id]
    #
    #         k = min(config.constants.MAGIC_FIFO_NUM, len(neighbour_list))
    #         sorted_neighbours = sorted(neighbour_list, key=lambda node: self.node_limit[node][0], reverse=True)[:k]
    #
    #         for neighbour in sorted_neighbours:
    #             neighbour_has_table = self.node_owntable[neighbour]
    #             both_one = np.logical_and(neighbour_has_table, require_table)
    #             coordinates = np.where(both_one)
    #             matching_coordinates = list(zip(coordinates[0], coordinates[1]))[:self.node_limit[neighbour][0]//2]
    #
    #             command_send = Command(command=config.command.SEND, extra_information=matching_coordinates,
    #                                    flag=config.command.GREEDY_FASTEST_FAST, recv=thread_id)
    #             self.send_data(thread_id=neighbour, data=command_send)
    #             self.send_num+=1
    #     else:
    #         data = Command(command=config.command.OWNTABLE2TRACKER,
    #                        extra_information=None, flag=config.command.GREEDY_FASTEST_FAST)
    #         self.send_data_all(data=data)

    def random_fifo(self, round=None):
        if round!=None:
            #transfer to neighborhood_matrix
            node_list = list(self.threads.keys())
            matrix_size = len(node_list)
            neighborhood_matrix = np.zeros((matrix_size, matrix_size), dtype=int)
            node_index = {node: idx for idx, node in enumerate(node_list)} #quick check dict
            for node, neighbours in self.neighbour_list.items():
                if node in node_index:
                    node_idx = node_index[node]
                    for neighbour in neighbours:
                        if neighbour in node_index:
                            neighbour_idx = node_index[neighbour]
                            neighborhood_matrix[node_idx][neighbour_idx] = 1
            print('neighborhood_matrix')
            print(neighborhood_matrix)

            #transfer to chunks_matrix
            total_chunks = sum(matrix.shape[0] for matrix in self.node_owntable.values())
            chunks_matrix = np.zeros((matrix_size, total_chunks), dtype=int)

            for node_id, matrix in self.node_owntable.items():
                node_idx = node_index[node_id]
                row_idx = 0
                for chunk in range(matrix.shape[1]):
                    for chunk_j in range(matrix.shape[0]):
                        chunks_matrix[node_idx, row_idx] = 1 if matrix[chunk_j, chunk] else 0
                        row_idx += 1

            #transfer to uplink_vector and downlink_vector
            uplink_vector = np.zeros(matrix_size, dtype=int)
            downlink_vector = np.zeros(matrix_size, dtype=int)
            for node_id in self.node_limit.keys():
                node_idx = node_index[node_id]
                uplink_vector[node_idx],downlink_vector[node_idx]=self.node_limit[node_id]

            Random_FIFO_instance = Random_FIFO(neighborhood_matrix, chunks_matrix, uplink_vector, downlink_vector, P_value=config.constants.MIN_CHUNK_LIMIT+config.constants.GENERATE_FILE_SIZE/config.constants.CHUNK_SIZE, K_value=config.constants.MAGIC_FIFO_NUM)
            self.algo_result_dict = Random_FIFO_instance.Operation()

            #calculate the utilization
            self.bw_utilization[self.algo_flag]= [len(self.algo_result_dict[key]) / self.bw_max * 100 for key in self.algo_result_dict]

            print(self.algo_result_dict)
            print(self.bw_utilization)
        else:
            data = Command(command=config.command.OWNTABLE2TRACKER,
                           extra_information=None, flag=config.command.RANDOM_FIFO)
            self.send_data_all(data=data)

    def random_fastest_fast(self, round=None):
        if round!=None:
            #transfer to neighborhood_matrix
            node_list = list(self.threads.keys())
            matrix_size = len(node_list)
            neighborhood_matrix = np.zeros((matrix_size, matrix_size), dtype=int)
            node_index = {node: idx for idx, node in enumerate(node_list)} #quick check dict
            for node, neighbours in self.neighbour_list.items():
                if node in node_index:
                    node_idx = node_index[node]
                    for neighbour in neighbours:
                        if neighbour in node_index:
                            neighbour_idx = node_index[neighbour]
                            neighborhood_matrix[node_idx][neighbour_idx] = 1
            print('neighborhood_matrix')
            print(neighborhood_matrix)

            #transfer to chunks_matrix
            total_chunks = sum(matrix.shape[0] for matrix in self.node_owntable.values())
            chunks_matrix = np.zeros((matrix_size, total_chunks), dtype=int)

            for node_id, matrix in self.node_owntable.items():
                node_idx = node_index[node_id]
                row_idx = 0
                for chunk in range(matrix.shape[1]):
                    for chunk_j in range(matrix.shape[0]):
                        chunks_matrix[node_idx, row_idx] = 1 if matrix[chunk_j, chunk] else 0
                        row_idx += 1

            #transfer to uplink_vector and downlink_vector
            uplink_vector = np.zeros(matrix_size, dtype=int)
            downlink_vector = np.zeros(matrix_size, dtype=int)
            for node_id in self.node_limit.keys():
                node_idx = node_index[node_id]
                uplink_vector[node_idx],downlink_vector[node_idx]=self.node_limit[node_id]

            Random_FastestFirst_instance = Random_FastestFirst(neighborhood_matrix, chunks_matrix, uplink_vector, downlink_vector, P_value=config.constants.MIN_CHUNK_LIMIT+config.constants.GENERATE_FILE_SIZE/config.constants.CHUNK_SIZE, K_value=config.constants.MAGIC_FIFO_NUM)
            self.algo_result_dict = Random_FastestFirst_instance.Operation()
            # calculate the utilization
            self.bw_utilization[self.algo_flag]= [len(self.algo_result_dict[key]) / self.bw_max * 100 for key in self.algo_result_dict]

            print(self.algo_result_dict)
            print(self.bw_utilization)
        else:
            data = Command(command=config.command.OWNTABLE2TRACKER,
                           extra_information=None, flag=config.command.RANDOM_FASTEST_FAST)
            self.send_data_all(data=data)

    def greedy_fastest_fast(self, round=None):
        if round!=None:
            #transfer to neighborhood_matrix
            node_list = list(self.threads.keys())
            matrix_size = len(node_list)
            neighborhood_matrix = np.zeros((matrix_size, matrix_size), dtype=int)
            node_index = {node: idx for idx, node in enumerate(node_list)} #quick check dict
            for node, neighbours in self.neighbour_list.items():
                if node in node_index:
                    node_idx = node_index[node]
                    for neighbour in neighbours:
                        if neighbour in node_index:
                            neighbour_idx = node_index[neighbour]
                            neighborhood_matrix[node_idx][neighbour_idx] = 1
            print('neighborhood_matrix')
            print(neighborhood_matrix)

            #transfer to chunks_matrix
            total_chunks = sum(matrix.shape[0] for matrix in self.node_owntable.values())
            chunks_matrix = np.zeros((matrix_size, total_chunks), dtype=int)

            for node_id, matrix in self.node_owntable.items():
                node_idx = node_index[node_id]
                row_idx = 0
                for chunk in range(matrix.shape[1]):
                    for chunk_j in range(matrix.shape[0]):
                        chunks_matrix[node_idx, row_idx] = 1 if matrix[chunk_j, chunk] else 0
                        row_idx += 1


            #transfer to uplink_vector and downlink_vector
            uplink_vector = np.zeros(matrix_size, dtype=int)
            downlink_vector = np.zeros(matrix_size, dtype=int)
            for node_id in self.node_limit.keys():
                node_idx = node_index[node_id]
                uplink_vector[node_idx],downlink_vector[node_idx]=self.node_limit[node_id]

            Greedy_FastestFirst_instance = Greedy_FastestFirst(neighborhood_matrix, chunks_matrix, uplink_vector, downlink_vector, P_value=config.constants.MIN_CHUNK_LIMIT+config.constants.GENERATE_FILE_SIZE/config.constants.CHUNK_SIZE, K_value=config.constants.MAGIC_FIFO_NUM)
            self.algo_result_dict = Greedy_FastestFirst_instance.Operation()
            # calculate the utilization
            self.bw_utilization[self.algo_flag]= [len(self.algo_result_dict[key]) / self.bw_max * 100 for key in self.algo_result_dict]

            print(self.algo_result_dict)
            print(self.bw_utilization)
        else:
            data = Command(command=config.command.OWNTABLE2TRACKER,
                           extra_information=None, flag=config.command.GREEDY_FASTEST_FAST)
            self.send_data_all(data=data)

    # scheduling algorithm4: Max flow
    def max_flow(self, round=None):
        if round!=None:
            #transfer to neighborhood_matrix
            node_list = list(self.threads.keys())
            matrix_size = len(node_list)
            neighborhood_matrix = np.zeros((matrix_size, matrix_size), dtype=int)
            node_index = {node: idx for idx, node in enumerate(node_list)} #quick check dict
            for node, neighbours in self.neighbour_list.items():
                if node in node_index:
                    node_idx = node_index[node]
                    for neighbour in neighbours:
                        if neighbour in node_index:
                            neighbour_idx = node_index[neighbour]
                            neighborhood_matrix[node_idx][neighbour_idx] = 1
            print('neighborhood_matrix')
            print(neighborhood_matrix)

            #transfer to chunks_matrix
            total_chunks = sum(matrix.shape[0] for matrix in self.node_owntable.values())
            chunks_matrix = np.zeros((matrix_size, total_chunks), dtype=int)

            for node_id, matrix in self.node_owntable.items():
                node_idx = node_index[node_id]
                row_idx = 0
                for chunk in range(matrix.shape[1]):
                    for chunk_j in range(matrix.shape[0]):
                        chunks_matrix[node_idx, row_idx] = 1 if matrix[chunk_j, chunk] else 0
                        row_idx += 1

            #transfer to uplink_vector and downlink_vector
            uplink_vector = np.zeros(matrix_size, dtype=int)
            downlink_vector = np.zeros(matrix_size, dtype=int)
            for node_id in self.node_limit.keys():
                node_idx = node_index[node_id]
                uplink_vector[node_idx],downlink_vector[node_idx]=self.node_limit[node_id]

            Max_flow_instance = Max_Flow(neighborhood_matrix, chunks_matrix, uplink_vector, downlink_vector, P_value=config.constants.MIN_CHUNK_LIMIT+config.constants.GENERATE_FILE_SIZE/config.constants.CHUNK_SIZE)
            self.algo_result_dict = Max_flow_instance.operation()
            # calculate the utilization
            self.bw_utilization[self.algo_flag]= [len(self.algo_result_dict[key]) / self.bw_max * 100 for key in self.algo_result_dict]

            print(self.algo_result_dict)
            print(self.bw_utilization)
        else:
            data = Command(command=config.command.OWNTABLE2TRACKER,
                           extra_information=None, flag=config.command.MAX_FLOW)
            self.send_data_all(data=data)

    def choose_algo(self, command):
        if command == '0' or command == 'send':  # nomal torrent share file
            self.torrent()
        elif command == '1' or command == 'randomfifo':  # scheduling algorithm1: Heuristics Random-FIFO
            self.algo_flag = 'randomfifo'
            self.random_fifo()

        elif command == '2' or command == 'randomfastestfast':  # scheduling algorithm2: Heuristics Random-Fastest-Fast
            self.algo_flag = 'randomfastestfast'
            self.random_fastest_fast()

        elif command == '3' or command == 'greedyfastestfast':  # scheduling algorithm3: Heuristics Greedy-Fastest-Fast
            self.algo_flag = 'greedyfastestfast'
            self.greedy_fastest_fast()

        elif command == '4' or command == 'maxflow':  # scheduling algorithm4: Max flow
            self.algo_flag = 'maxflow'
            self.max_flow()

        elif command == '5' or command == 'flooding':
            self.algo_flag = 'flooding'
            self.random_fifo()

        else:
            print("WRONG COMMAND!")
            self.input_flag = True

    def plot(self):
        print("Plot y/n?")
        command = input()
        if command!='y':
            return

        plt.figure()
        for key in self.increments:
            y_values = range(len(self.increments[key]))
            print(f"Key: {key}, Length: {len(self.increments[key])}, Value: {self.increments[key]}")
            plt.plot(self.increments[key], range(len(self.increments[key])), label=key)
            plt.vlines(self.increments[key][0], ymin=0, ymax=y_values[0], color='gray', linestyle='--')
            plt.vlines(self.increments[key][-1], ymin=0, ymax=y_values[-1], color='gray', linestyle='--')

        plt.legend()
        plt.xlabel('Seconds')
        plt.ylabel(f'Number of nodes that hold at leat {config.constants.MIN_CHUNK_LIMIT} Chunks')

        plt.title(
            f'N={config.constants.TEST_NODE_NUM}, C={config.constants.GENERATE_FILE_SIZE / config.constants.CHUNK_SIZE * config.constants.TEST_NODE_NUM}, K={config.constants.MAGIC_FIFO_NUM}, R={config.constants.RANDOM_LAG_TIMES}, P={config.constants.MIN_CHUNK_LIMIT}, Min_connect={config.constants.MIN_NODE_CONNECTION}')
        plt.show()

        plt.figure()
        for key in self.bw_utilization:
            plt.plot(self.bw_utilization_time[key], self.bw_utilization[key], label=key)
        plt.legend()
        plt.xlabel('Seconds')
        plt.ylabel('Bandwidth utilization percentage')

        plt.title(
            f'N={config.constants.TEST_NODE_NUM}, C={config.constants.GENERATE_FILE_SIZE / config.constants.CHUNK_SIZE * config.constants.TEST_NODE_NUM}, K={config.constants.MAGIC_FIFO_NUM}, R={config.constants.RANDOM_LAG_TIMES}, P={config.constants.MIN_CHUNK_LIMIT}, Min_connect={config.constants.MIN_NODE_CONNECTION}')
        plt.show()


    def run(self):
        log_content = f"***************** Tracker program started just right now! *****************"
        log(node_id=0, content=log_content, is_tracker=True)
        self.start_accepting_connections()

        # receive command from the other
        while True:
            try:
                # if the node num equal to the setting num, start test
                if len(self.neighbour_list.keys()) == config.constants.TEST_NODE_NUM and self.input_flag:
                    self.input_flag = False  # stop input command
                    # send the network node list to every node
                    node_list = list(self.threads.keys())
                    data = Command(command=config.command.NETWORK_LIST, extra_information=node_list)
                    self.send_data_all(data=data)
                    self.plot()
                    #calculate the max bandwidth of the network
                    sum_i = 0
                    sum_j = 0
                    for key in self.node_limit:
                        i, j = self.node_limit[key]
                        sum_i += i
                        sum_j += j
                    self.bw_max=min(sum_i, sum_j)

                    print("ENTER YOUR COMMAND!")
                    command = input()
                    log_content = f"ALGORITHM IS: {command}"
                    log(node_id=0, content=log_content, is_tracker=True)
                    if command in self.bw_utilization:
                        self.bw_utilization[command]=[]
                    if command in self.bw_utilization_time:
                        self.bw_utilization_time[command]=[]
                    self.start_time = time.time()
                    self.choose_algo(command)


                thread_id, received_data = self.receive_queue.get_nowait()
                received_data = Message.decode(received_data)
                # Process the received data
                self.process_command(received_data, thread_id)
                #print(self.ok_torrent_time)
                #finish one slot
                if max(self.finish_down_node_num,self.finish_up_node_num)+self.finish_program_num==config.constants.TEST_NODE_NUM\
                        or self.send_count==self.send_num:
                #if max(self.finish_down_node_num,self.finish_up_node_num) == config.constants.TEST_NODE_NUM:

                    self.time_slot += 1
                    self.node_owntable_old = self.node_owntable.copy()
                    self.finish_up_node_num=0
                    self.finish_down_node_num=0
                    data = Command(command=config.command.NEXTSLOT, extra_information=self.time_slot)
                    self.send_data_all(data=data)
                    self.send_num = 0
                    self.send_count = -1

                    self.algo_send(round=self.time_slot + 1)

                    # if self.algo_flag=='maxflow':
                    #     self.algo_send(round=self.time_slot+1)
                    # elif self.algo_flag:
                    #     self.own_table_num = 0
                    #     self.choose_algo(self.algo_flag)

                #finish all
                if self.ok_torrent == config.constants.TEST_NODE_NUM or (self.time_slot>=len(self.algo_result_dict) and self.algo_result_dict):
                    # print('self.torrent()')
                    # self.torrent()
                    # self.algo_flag=None
                    # self.ok_torrent=0

                    self.ok_torrent=0
                    self.ok_torrent_set=set()
                    self.finish_time += (self.time_slot * config.constants.TIME_UNIT_LENGTH)
                    print(f"PROGRAM FINISHED! TOTAL TIME: {self.finish_time:.3f} seconds")
                    print(f"TOTAL TIME SLOT: {self.time_slot:.0f}")
                    log_content = f"PROGRAM FINISHED! TOTAL TIME: {self.finish_time:.3f} seconds"
                    log(node_id=0, content=log_content, is_tracker=True)
                    log_content = f"TOTAL TIME SLOT: {self.time_slot:.0f}"
                    log(node_id=0, content=log_content, is_tracker=True)
                    data = Command(command=config.command.CLEAR_ALL, extra_information=node_list)
                    self.send_data_all(data=data)

                    self.own_table_num = 0
                    self.finish_program_num = 0
                    self.ok_torrent = 0
                    self.finish_time = 0
                    self.input_flag = True  # for input command manualy
                    self.start_time = 0
                    self.debug_count = 0
                    self.time_slot = 0

                    self.send_num = 0
                    self.send_count = -1

                    self.increments[self.algo_flag] = self.ok_torrent_time
                    for key in self.increments:
                        print(f"Key: {key}, Value: {self.increments[key]}")


                    self.algo_flag = None
                    self.ok_torrent_time=[]
                    # clear queue
                    while not self.receive_queue.empty():
                        self.receive_queue.get()
                        self.receive_queue.task_done()

                #
                #
                # # whole program finished
                # if self.finish_program_num == len(self.threads.keys()) and self.input_flag == False:
                #     self.finish_time+=(self.time_slot*config.constants.TIME_UNIT_LENGTH)
                #     print(f"PROGRAM FINISHED! TOTAL TIME: {self.finish_time:.3f} seconds")
                #     print(f"TOTAL TIME SLOT: {self.time_slot:.0f}")
                #     log_content = f"PROGRAM FINISHED! TOTAL TIME: {self.finish_time:.3f} seconds"
                #     log(node_id=0, content=log_content, is_tracker=True)
                #     log_content = f"TOTAL TIME SLOT: {self.time_slot:.0f}"
                #     log(node_id=0, content=log_content, is_tracker=True)
                #     data = Command(command=config.command.CLEAR_ALL, extra_information=node_list)
                #     self.send_data_all(data=data)
                #
                #     self.own_table_num = 0
                #     self.finish_program_num = 0
                #     self.ok_torrent = 0
                #     self.finish_time = 0
                #     self.input_flag = True  # for input command manualy
                #     self.start_time = 0
                #     self.debug_count = 0
                #     self.time_slot=0
                #     self.algo_flag=None
                #     self.send_num = 0
                #     self.send_count = -1
                #
                #     # clear queue
                #     while not self.receive_queue.empty():
                #         self.receive_queue.get()
                #         self.receive_queue.task_done()
            except queue.Empty:
                continue  # No data received, continue to the next iteration











if __name__ == '__main__':
    t = Tracker()
    t.run()