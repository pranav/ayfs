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
    def add_test_data(self):
        # 40755
        self.create("/", 16877)
        # 100644
        self.create("/test", 33188)

    def __init__(self, etcd_host='10.0.0.4'):
        self.FILE_PREFIX = '/files'
        self.DIR_INFO = '__ayfs_dir_info'
        self.etcd = etcd.Client(host=etcd_host)
        self.add_test_data()

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
            "size": 0
        }
        self.set_file(path, file_dict)
        logging.info("Created ", path)
        return 0

    def destroy(self, path):
        try:
            self.delete_file(path)
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
        pass

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
        pass

    def utimens(self, path, times=None):
        pass

    def write(self, path, data, offset, fh):
        pass


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print('usage: %s <host> <mountpoint>' % sys.argv[0])
        exit(1)

    fuse = FUSE(AYFS(sys.argv[1]), sys.argv[2], foreground=True, nothreads=True)