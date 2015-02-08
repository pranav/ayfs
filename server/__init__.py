import time
import sys
import struct
import etcd
import socket
import Queue
import threading
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler(sys.stderr))


class Server():
    def __init__(self, etcd_host='127.0.0.1', etcd_port=4001):
        self.BUFFER_SIZE = 10000000
        self.WORKERS = 4
        self.received_queue = Queue.Queue()
        self.send_queue = Queue.Queue()
        self.client_queue = Queue.Queue()

        self.receive_port = 4100
        self.s_receiver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.recipient_host = None

        self.etcd_host = etcd_host
        self.etcd_port = etcd_port
        self.etcd = etcd.Client(host=self.etcd_host, port=self.etcd_port)
        self.etcd.write("/active_nodes/%s" % socket.gethostname(), 0, ttl=1)

    def start(self):
        self.start_heartbeat_thread()
        self.start_receiver_thread()
        self.start_processor_workers()
        self.start_recipient_thread()
        self.start_client_sender_thread()
        self.send()

    def start_client_sender_thread(self):
        t = threading.Thread(target=self.client_sender)
        t.daemon = True
        t.start()

    def client_sender(self):
        while True:
            block_ip_data = self.client_queue.get(True)
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.sendto(block_ip_data[1], (block_ip_data[0], 4101))
            sock.close()

    def start_recipient_thread(self):
        t = threading.Thread(target=self.find_recipient)
        t.daemon = True
        t.start()

    def find_recipient(self):
        while True:
            try:
                if not (self.recipient_host is None):
                    if int(self.etcd.get("/nodes/%s" % self.recipient_host).value) == 0:
                        self.etcd.write("/active_nodes/%s" % host, 0, ttl=1)
                        continue
            except KeyError:
                if not (self.recipient_host is None):
                    logger.info("Lost connection to %s" % self.recipient_host)
                    self.recipient_host = None
                pass
            try:
                all_nodes = set(self.get_all_nodes())
                active_nodes = set(self.get_active_nodes())
                inactive_nodes = all_nodes - active_nodes
                for node in inactive_nodes:
                    host = Server.key2host(node)
                    if host != socket.gethostname():
                        self.etcd.write("/active_nodes/%s" % host, 0, ttl=1)
                        self.recipient_host = host
                        logger.info("Connected to %s" % host)
            except IndexError:
                pass

    @staticmethod
    def key2host(nodekey):
        return nodekey.split('/')[2]

    def get_all_nodes(self):
        return [node.key for node in self.etcd.read("/nodes", recursive=True).children]

    def get_active_nodes(self):
        return [node.key for node in self.etcd.read("/active_nodes", recursive=True).children]

    def register_etcd(self):
        """
        Set node to 0
        """
        key = "/nodes/%s" % socket.gethostname()
        val = 0
        self.etcd.write("/nodes/%s" % socket.gethostname(), val, ttl=1)

    def deregister_etcd(self):
        self.etcd.delete("/nodes/%s")

    def start_heartbeat_thread(self):
        t = threading.Thread(target=self.heartbeat)
        t.daemon = True
        t.start()

    def heartbeat(self):
        while True:
            time.sleep(0.2)
            self.register_etcd()

    def start_receiver_thread(self):
        receiver_thread = threading.Thread(target=self._start_listening)
        receiver_thread.daemon = True
        receiver_thread.start()
        logger.debug("Started Receiver Thread")

    def _start_listening(self):
        self.s_receiver.bind(('0.0.0.0', self.receive_port))
        while True:
            data = self.s_receiver.recv(self.BUFFER_SIZE)
            self.received_queue.put_nowait(data)
            logger.debug("Received %s bytes" % len(data))

    def start_processor_workers(self):
        for worker_id in range(0, self.WORKERS):
            t = threading.Thread(target=self.worker)
            t.daemon = True
            t.start()
            logger.debug("Started worker %s" % worker_id)

    def unpack(self, raw_block):
        """
        Returns raw block typle
        :param raw_block:
        :return:tuple (block_id, data_size, data)
        """
        block_id, block_size, data = struct.unpack("II1000s", raw_block)
        return block_id, block_size, data[:block_size]

    def worker(self):
        while True:
            block = self.received_queue.get(True, timeout=None)
            block_id, data_size, block_data = self.unpack(block)

            for block_ip_id in self.get_list_of_wanted_blocks():
                if int(block_id) == int(block_ip_id[1]):
                    self.client_queue.put_nowait((block_ip_id[0], block))
                    try:
                        self.etcd.delete("/wanted_blocks/%d" % int(block_ip_id[1]))
                    except KeyError:
                        pass
                    logger.info("Added %s to client_queue for %s" % (block_ip_id[1], block_ip_id[0]))
            self.send_queue.put_nowait(block)
            logger.debug("Put block in send_queue")

    def send(self):
        while True:
            block = self.send_queue.get(True, timeout=None)
            logger.debug("Got block from send_queue")
            s_send = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sent_bytes = s_send.sendto(block, (self.recipient_host, self.receive_port))
            s_send.close()
            logger.debug("Sent %s bytes to %s:%s" % (sent_bytes, self.recipient_host, self.receive_port))

    def get_list_of_wanted_blocks(self):
        """
        :return:tuple (ip, block)
        """
        block_ids = []
        for node in self.etcd.read("/wanted_blocks", recursive=True).children:
            block_ids.append((node.value, node.key.split('/')[2]))
        return block_ids

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.deregister_etcd()

