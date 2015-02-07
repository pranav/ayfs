import etcd
import socket
import Queue
import threading
import logging


class Server():
    def __init__(self):
        logging.basicConfig(level=logging.INFO)
        self.BUFFER_SIZE = 16384
        self.WORKERS = 1
        self.etcd = etcd.Client()
        self.received_queue = Queue.Queue()
        self.send_queue = Queue.Queue()

        self.receive_port = 4100
        self.s_receiver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def start(self):
        self.start_heartbeat_thread()
        self.start_receiver_thread()
        self.start_processor_workers()
        self.send()

    def register_etcd(self):
        self.etcd.write("/nodes/%s" % socket.gethostname(), 1, ttl=2)

    def deregister_etcd(self):
        self.etcd.delete("/nodes/%s")

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.deregister_etcd()

    def start_heartbeat_thread(self):
        t = threading.Thread(target=self.heartbeat)
        t.daemon = True
        t.start()

    def heartbeat(self):
        while True:
            self.register_etcd()

    def start_receiver_thread(self):
        receiver_thread = threading.Thread(target=self._start_listening)
        receiver_thread.daemon = True
        receiver_thread.start()
        logging.debug("Started Receiver Thread")

    def _start_listening(self):
        self.s_receiver.bind(('0.0.0.0', self.receive_port))
        while True:
            data = self.s_receiver.recv(self.BUFFER_SIZE)
            self.received_queue.put_nowait(data)
            logging.info("Received %s bytes" % len(data))

    def start_processor_workers(self):
        for worker_id in range(0, self.WORKERS):
            t = threading.Thread(target=self.worker)
            t.daemon = True
            t.start()
            logging.debug("Started worker %s" % worker_id)

    def worker(self):
        while True:
            block = self.received_queue.get(True, timeout=None)
            self.send_queue.put_nowait(block)
            logging.debug("Put block in send_queue")

    def send(self):
        while True:
            block = self.send_queue.get(True, timeout=None)
            logging.debug("Got block from send_queue")
            s_send = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            #TODO: figure out who to send to.
            sent_bytes = s_send.sendto(block, ('0.0.0.0', self.receive_port))
            logging.info("Sent %s bytes to %s:%s" % (sent_bytes, '0.0.0.0', self.receive_port))


