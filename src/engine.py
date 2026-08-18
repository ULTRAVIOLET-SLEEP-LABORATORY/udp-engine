#! /usr/bin/env python3

# udp_engine/src/engine.py
# Author: Joshua Darrow     07.20.2026

'''
The basic structure of the code is as follows.
The UDPEngine consists of two ports: the data port and control port.
The UDPEngine also consists of four queues, two for each port.
These are useful for communicating with biosignal sensors among other purposes.

Finally, there are four threads whose jobs are something like this:
1) Wait around listening for data
2) As soon as data is received, run a list of operators/functions on the data.

An example of this would be a thread that listens to one of the sockets.
As soon as the socket receives data, it awakens the thread, which pushes the
data to a queue. It's also possible to hook other functions to be called by
the thread, but these functions should be made to run as fast as possible.
'''

import socket
import threading
from blocking_deque import BlockingDeque


class UDPEngine:
    '''Engine for network operations and data structures.
    Manages queues.'''

    def __init__(self, remote_ip, remote_control_port, remote_data_port, local_ip, local_control_port, local_data_port):
        '''Initialize all of the networking sockets and queues'''

        self.MAX_UDP_SIZE = 65535
        self.remote_ip = remote_ip
        self.remote_control_port = remote_control_port
        self.remote_data_prot = remote_data_port
        self.local_ip = local_ip
        self.local_control_port = local_control_port
        self.local_data_port = local_data_port

        self.control_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)          # create control socket
        self.control_socket.bind((local_ip, local_control_port))                                          # set control socket address
        self.control_rx_queue = BlockingDeque(maxlen=100)                                           # create control receive queue
        self.control_tx_queue = BlockingDeque(maxlen=100)                                           # create control send queue

        self.data_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)             # create data socket
        self.data_socket.bind((local_ip, local_data_port))                                               # set data socket address
        self.data_rx_queue = BlockingDeque(maxlen=100)                                              # create data receive queue
        self.out_tx_queue = BlockingDeque(maxlen=100)                                               # create generic out queue


        self._stop_event = threading.Event()
        self._recv_timeout = 0.5  # seconds; lets loops check _stop_event periodically
        self.control_socket.settimeout(self._recv_timeout)
        self.data_socket.settimeout(self._recv_timeout)

        # List to hold functions that process each stream.
        # Avoid having operators add other operators to the list.
        self.control_rx_operators = []
        self.control_tx_operators = []
        self.data_rx_operators = []
        self.data_tx_operators = []


    def start(self):
        '''start all of the threads'''
        self._stop_event.clear()

        # hook the default stream processors
        self.control_rx_operators.append(self.control_rx_queue.append)
        self.control_tx_operators.append(self.control_socket.sendto)
        self.data_rx_operators.append(self.data_rx_queue.append)
        #self.data_tx_operators.append(self.data_socket.sendto)

        # initialize and start threads
        self.control_rx_thread = threading.Thread(target=self._run_control_rx_queue, daemon=True)
        self.control_tx_thread = threading.Thread(target=self._run_control_tx_queue, daemon=True)
        self.data_rx_thread = threading.Thread(target=self._run_data_rx_queue, daemon=True)
        self.out_tx_thread = threading.Thread(target=self._run_out_tx_queue, daemon=True)

        self.control_rx_thread.start()
        self.control_tx_thread.start()
        self.data_rx_thread.start()
        self.out_tx_thread.start()


    def stop(self):
        self._stop_event.set()

        # stop all threads
        for t in (self.control_rx_thread, self.control_tx_thread, self.data_rx_thread, self.out_tx_thread):
            t.join(timeout=2.0)

        self.control_socket.close()     # close sockets
        self.data_socket.close()


    def _run_control_rx_queue(self):
        '''listen to the control socket and push to the control queue'''

        while not self._stop_event.is_set():
            try:
                received_command, _addr = self.control_socket.recvfrom(self.MAX_UDP_SIZE)           # Receive from socket
            except socket.timeout:
                continue
            logger.debug("[Control RX Queue] Received: %r", received_command)                       # log reception
            for function in self.control_rx_operators:                                              # iterate though operators and run them on the received
                try:
                    function(received_command)                                                      # e.g. push to queue for processing
                except Exception:
                    logger.exception("operator %r raised", function)
            

    def _run_control_tx_queue(self):
        '''listen to the control queue and send commands to board (remote) via socket'''

        while not self._stop_event.is_set():
            try:
                command = self.control_tx_queue.popleft(timeout=self._recv_timeout)
            except TimeoutError:
                continue
            logger.debug("[Control TX Queue] Sending: %r", received_command)
            for function in self.control_tx_operators:
                try:
                    function(command, (self.remote_ip, self.remote_control_port))   # send data via socket, etc.
                except Exception:
                    logger.exception("operator %r raised", function)


    def _run_data_rx_queue(self):
        '''listen to data socket and push data asap to data queue for processing'''

        while not self._stop_event.is_set():
            try:
                received_data, _addr = self.data_socket.recvfrom(self.MAX_UDP_SIZE)
            except socket.timeout:
                continue
            logger.debug("[Data RX Queue] Received: %r", received_data)
            for function in self.data_rx_operators:
                try:
                    function(received_data)
                except Exception:
                    logger.exception("operator %r raised", function)


    def _run_out_tx_queue(self):
        '''listen to the out queue and send data out e.g. via lsl'''

        while not self._stop_event.is_set():
            try:
                data = self.out_tx_queue.popleft(timeout=self._recv_timeout)
            except TimeoutError:
                continue
            logger.debug("[Data TX Queue] Sending: %r", data)
            for function in self.data_tx_operators:
                try:
                    function(data, (self.remote_ip, self.remote_control_port))   # send data via socket, etc.
                except Exception:
                    logger.exception("operator %r raised", function)

