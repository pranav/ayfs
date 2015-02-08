import time
import logging
import server

if __name__ == '__main__':
    s = server.Server(etcd_host='etcd')
    s.start()
