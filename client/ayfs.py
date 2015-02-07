import sys
from fuse import FUSE, Operations


class AYFS(Operations):
    def __init__(self):
        pass

    def chmod(self, path, mode):
        pass

    def chown(self, path, uid, gid):
        pass

    def create(self, path, mode, fi=None):
        pass

    def destroy(self, path):
        pass

    def getattr(self, path, fh=None):
        pass

    def mkdir(self, path, mode):
        pass

    def read(self, path, size, offset, fh):
        pass

    def readdir(self, path, fh):
        pass

    def readlink(self, path):
        pass

    def rename(self, old, new):
        pass

    def rmdir(self, path):
        pass

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