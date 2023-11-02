import socket
import random
import warnings
import os
from datetime import datetime
from configs import CFG, Config
config = Config.from_json(CFG)
import threading
import queue
import select
import struct
from messages.message import Message

# global variables
used_ports = []

class ConnectionThread(threading.Thread):
    def __init__(self, id, send_queue, receive_queue, conn, cleanup_callback):
        super().__init__()
        self.id = id  # Unique identifier for this thread
        self.send_queue = send_queue
        self.receive_queue = receive_queue
        self.conn = conn
        self.cleanup_callback = cleanup_callback
        self.conn.setblocking(0)  # Set socket to non-blocking mode
        self.send_buffer = b''  # Initialize send buffer
        self.recv_buffer = b''

    def cleanup(self):
        self.cleanup_callback(self.id)

    def run(self):
        inputs = [self.conn]
        outputs = []
        while inputs:
            try:
                if not self.send_buffer:
                    message = self.send_queue.get_nowait()
                    # Prefix each message with a 4-byte length (network byte order)
                    self.send_buffer = struct.pack('!I', len(message)) + message
                    outputs.append(self.conn)
            except queue.Empty:
                pass

            readable, writable, exceptional = select.select(inputs, outputs, inputs, 0.1)

            for s in writable:
                try:
                    sent = s.send(self.send_buffer)
                    self.send_buffer = self.send_buffer[sent:]
                    if not self.send_buffer:
                        outputs.remove(s)
                except (socket.error, BlockingIOError):
                    pass

            for s in readable:
                try:
                    # Receive data chunk
                    data = s.recv(4096)
                    if data:
                        self.recv_buffer += data
                        # Process complete messages
                        while len(self.recv_buffer) >= 4:  # Assuming 4-byte header
                            # Extract the message length
                            message_len = struct.unpack('!I', self.recv_buffer[:4])[0]
                            # Check if the buffer has enough bytes for the message
                            if len(self.recv_buffer) >= 4 + message_len:
                                # Extract the message
                                message = self.recv_buffer[4:4 + message_len]
                                self.receive_queue.put((self.id, message))
                                # Remove the message from buffer
                                self.recv_buffer = self.recv_buffer[4 + message_len:]
                            else:
                                break
                    else:
                        self.cleanup()
                        inputs.remove(s)
                        break
                except (socket.error, BlockingIOError):
                    break

            for s in exceptional:
                self.cleanup()
                inputs.remove(s)
                if s in outputs:
                    outputs.remove(s)


def set_socket(port: int) -> socket.socket:
    '''
    This function creates a new TCP socket

    :param port: port number
    :return: A socket object with an unused port number
    '''
    sock = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(('localhost', port))
    sock.listen()  # Add this line to listen for incoming connections
    used_ports.append(port)

    return sock

def accept_connection(sock: socket.socket) -> socket.socket:
    '''
    This function accepts a new connection on the given socket

    :param sock: socket
    :return: A new socket object representing the accepted connection
    '''
    conn, addr = sock.accept()
    print(f'Connection from {addr}')
    return conn


def free_socket(sock: socket.socket):
    '''
    This function free a socket to be able to be used by others

    :param sock: socket
    :return:
    '''
    used_ports.remove(sock.getsockname()[1])
    sock.close()

def generate_random_port() -> int:
    '''
    This function generates a new(unused) random port number

    :return: a random integer in range of [1024, 65535]
    '''
    available_ports = config.constants.AVAILABLE_PORTS_RANGE
    rand_port = random.randint(available_ports[0], available_ports[1])
    while rand_port in used_ports:
        rand_port = random.randint(available_ports[0], available_ports[1])

    return rand_port

def parse_command(command: str):
    '''
    This function parses the input command

    :param command: A string which is the input command.
    :return: Command parts (mode, filename)
    '''
    parts = command.split(' ')
    try:
        if len(parts) == 4:
            mode = parts[2]
            filename = parts[3]
            return mode, filename
        elif len(parts) == 3:
            mode = parts[2]
            filename = ""
            return mode, filename
    except IndexError:
        warnings.warn("INVALID COMMAND ENTERED. TRY ANOTHER!")
        return

def log(node_id, content: str, is_tracker=False) -> None:
    '''
    This function is used for logging

    :param node_id: Since each node has an individual log file to be written in
    :param content: content to be written
    :return:
    '''
    if not os.path.exists(config.directory.logs_dir):
        os.makedirs(config.directory.logs_dir)

    # time
    now = datetime.now()
    current_time = now.strftime("%H:%M:%S")

    content = f"[{current_time}]  {content}\n"
    print(content)

    if is_tracker:
        node_logs_filename = config.directory.logs_dir + '_tracker.log'
    else:
        node_logs_filename = config.directory.logs_dir + 'node' + str(node_id) + '.log'
    if not os.path.exists(node_logs_filename):
        with open(node_logs_filename, 'w') as f:
            f.write(content)
            f.close()
    else:
        with open(node_logs_filename, 'a') as f:
            f.write(content)
            f.close()




