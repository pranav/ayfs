import threading
import socket
import Queue
import struct
import sys
import errno
import time
import json
import etcd
from pprint import pprint
from fuse import FUSE, Operations, FuseOSError
import logging

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler(sys.stderr))
logger.setLevel(logging.DEBUG)

fuse_logger = logging.getLogger('fuse.log-mixin')
fuse_logger.setLevel(logging.DEBUG)
fuse_logger.addHandler(logging.StreamHandler(sys.stderr))


class AYFS(Operations):
    """
    /files/<path to whatever>
    value: json
    example:
    {
        "mode": "0700",
        "uid": "001",
        "gid": "002",
    }

    """
    def __init__(self, etcd_host='10.0.0.4'):
        self.FILE_PREFIX = '/files'
        self.DIR_INFO = '__ayfs_dir_info'
        self.s_receiver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.etcd = etcd.Client(host=etcd_host)
        self.add_test_data()
        self.receive_queue = Queue.Queue()
        self.start_receiver_thread()

    def start_receiver_thread(self):
        t = threading.Thread(target=self.receiver_worker)
        t.daemon = True
        t.start()

    def get_etcd_tree(self, key):
        return self.etcd.read(key, recursive=True).children

    def receiver_worker(self):
        self.s_receiver.bind(('0.0.0.0', 4101))
        while True:
            data = self.s_receiver.recv(16000)
            block_id, useless_data = struct.unpack('I1000s', data)
            self.receive_queue.put_nowait(data)


    def get_requested_block_ids(self):
        my_ip = self.get_my_ipaddr()
        block_ids = []
        for node in self.get_etcd_tree('/wanted_blocks'):
            if str(node.value) == my_ip:
                block_ids.append(node.key.split('/')[2])
        return block_ids


    def get_file(self, path):
        if path.endswith('/'):
            path += self.DIR_INFO
        if path.endswith('.'):
            path = path[:-1] + self.DIR_INFO
        logger.info("Got %s" % path)
        return json.loads(self.etcd.get(self.FILE_PREFIX + path).value)

    def set_file(self, path, file_dict):
        if path.endswith('/'):
            self.etcd.write(self.FILE_PREFIX + path + self.DIR_INFO, json.dumps(file_dict))
        else:
            self.etcd.write(self.FILE_PREFIX + path, json.dumps(file_dict))
        logger.info("Added %s" % path)

    def delete_file(self, path):
        if path.endswith('/'):
            path += self.DIR_INFO
        self.etcd.delete(self.FILE_PREFIX + path)
        logger.info("Deleted %s" % path)

    def get_subfolder_paths(self, path):
        paths = []
        try:
            for node in self.etcd.read(self.FILE_PREFIX + path, recursive=True).children:
                paths.append(node.key.split("/")[2])
        except KeyError:
            pass
        return paths

    def chmod(self, path, mode):
        f = self.get_file(path)
        f['mode'] = mode
        self.set_file(path, f)
        logger.info("Set mode on %s to %s" % (path, mode))
        return 0

    def chown(self, path, uid, gid):
        f = self.get_file(path)
        f['uid'] = uid
        f['gid'] = gid
        self.set_file(path, f)
        return 0

    def create(self, path, mode, fi=None):
        current_time = int(time.time())
        #TODO: Fix uid/gid
        file_dict = {
            "mode": mode,
            "uid": 0,
            "gid": 0,
            "atime": current_time,
            "mtime": current_time,
            "size": 0,
            "blocks": ['0']
        }
        self.set_file(path, file_dict)
        logging.info("Created ", path)
        return 0

    def destroy(self, path):
        try:
            self.delete_file(path)
            logger.info("Deleted file: %s" % path)
        except KeyError:
            pass
        return 0

    def getattr(self, path, fh=None):
        try:
            logger.info("Getting attrs for %s" % path)
            f = self.get_file(path)
            attrs = ['atime', 'gid', 'mode', 'mtime', 'size', 'uid']
            attrs_dict = dict(("st_%s" % key, f[key]) for key in attrs)
            logger.info(attrs_dict)
            return attrs_dict
        except KeyError:
            raise FuseOSError(errno.ENOENT)

    def mkdir(self, path, mode):
        pass

    def read(self, path, size, offset, fh):
        blocks_ids = self.get_blocks_ids(path)
        for block_id in blocks_ids:
            self.etcd.write("/wanted_blocks/%d" % block_id, self.get_my_ipaddr())
        received_blocks = 0
        f = self.get_file(path)
        blocks = {}
        while received_blocks < len(f['blocks']):
            #TODO: Put back block if not for me, block I need
            raw_block = self.receive_queue.get(True)
            received_block_id, data = struct.unpack('I1000s', raw_block)
            blocks[received_block_id] = data
            logger.info("Received block: %s" % received_block_id)
            received_blocks += 1
        whole_file = self.assemble_file(blocks, blocks_ids)
        return whole_file

    def assemble_file(self, blocks, block_ids):
        buffer = blocks[block_ids[0]]
        for block_id in block_ids[1:]:
            buffer += blocks[block_id]
        return buffer

    def get_blocks_ids(self, path):
        f = self.get_file(path)
        return map(int, f['blocks'])

    def readdir(self, path, fh):
        logger.info("Readdir path %s" % path)
        dirs = ['.']
        logger.info("Found files %s" % dirs)
        dirs.extend(self.get_subfolder_paths(path))

        return dirs

    def readlink(self, path):
        pass

    def rename(self, old, new):
        f_old = self.get_file(old)
        self.set_file(old, f_old)
        self.delete_file(old)
        return 0

    def rmdir(self, path):
        self.delete_file(path)
        return 0

    def symlink(self, target, source):
        pass

    def truncate(self, path, length, fh=None):
        pass

    def unlink(self, path):
        self.destroy(path)
        return 0

    def utimens(self, path, times=None):
        pass

    def write(self, path, data, offset, fh):
        f = self.get_file(path)
        f['blocks'] = ['0']
        for i in range(0, len(data), 1000):
            block_id = self.get_new_block_id()
            block = struct.pack('I1000s', block_id, data[i:i+1000])
            f = self.upload_block(block, f, block_id)
            self.set_file(path, f)
        f['size'] = len(data)
        self.set_file(path, f)
        return 0

    def upload_block(self, block, f, block_id):
        node = list(self.etcd.read('/active_nodes/', recursive=True).children)[0]
        host = node.key.split('/')[2]
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.sendto(block, (host, 4100)) # Pray
        if f['blocks'] == ['0']:
            f['blocks'] = [block_id]
        else:
            f['blocks'].append(block_id)
        return f

    def get_new_block_id(self):
        """
        Python ints are 4 bytes.
        :return: int
        """
        block_id = 1
        for node in self.etcd.read("/files/", recursive=True).children:
            blocks = map(int, json.loads(node.value)['blocks'])
            block_id = max(block_id, max(blocks) + 1)
        return block_id

    def get_my_ipaddr(self):
        #TODO: Figure out the public ip differently
        return socket.gethostbyname(socket.gethostname())

    def add_test_data(self):
        # 40755
        self.create("/", 16877)
        # 100644
        self.create("/test", 33188)




if __name__ == '__main__':
    if len(sys.argv) != 3:
        print('usage: %s <host> <mountpoint>' % sys.argv[0])
        exit(1)

    fuse = FUSE(AYFS(sys.argv[1]), sys.argv[2], foreground=True, nothreads=True)