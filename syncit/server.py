import sys
import os
from os import path
import traceback
from collections import namedtuple

import picklemsg
from probity import probfile
from probity.backup import Backup

CHUNK_SIZE = 64 * 1024 # 64 KB

FileItem = namedtuple('FileItem', 'path checksum')

class Server(object):
    def __init__(self, root_path, remote):
        assert path.isdir(root_path)
        self.root_path = root_path
        self.remote = remote
        self.data_pool = Backup(path.join(self.root_path, 'objects'))

    def loop(self):
        for msg, payload in self.remote:
            try:
                if msg == 'quit':
                    self.remote.send('bye')
                    return

                func_name = 'msg_%s' % msg
                if hasattr(self, func_name):
                    method = getattr(self, func_name)
                    if callable(method):
                        method(payload)
                        continue

                raise ValueError("unknown message %r" % msg)

            except:
                try:
                    error_report = traceback.format_exc()
                except:
                    error_report = "[exception while formatting traceback]"
                self.remote.send('error', error_report)

    def msg_ping(self, payload):
        self.remote.send('pong', "server at %r" % self.root_path)

    def msg_stream_latest_version(self, payload):
        versions_path = path.join(self.root_path, 'versions')
        n = max(int(v) for v in os.listdir(versions_path))
        version_index_path = path.join(versions_path, str(n))

        self.remote.send('version_number', n)
        with open(version_index_path, 'rb') as f:
            for event in probfile.parse_file(f):
                file_meta = {
                    'path': event.path,
                    'checksum': event.checksum,
                    'size': event.size,
                }
                self.remote.send('file_begin', file_meta)

                h1, h2 = event.checksum[:2], event.checksum[2:]
                data_path = path.join(self.root_path, 'objects', h1, h2)
                with open(data_path, 'rb') as data_file:
                    while True:
                        chunk = data_file.read(CHUNK_SIZE)
                        if not chunk:
                            break
                        self.remote.send('file_chunk', chunk)
                self.remote.send('file_end')

        self.remote.send('done')

    def msg_merge(self, payload):
        versions_path = path.join(self.root_path, 'versions')
        n = max(int(v) for v in os.listdir(versions_path))
        assert n == payload

        local_bag = set()
        version_index_path = path.join(versions_path, str(n))
        with open(version_index_path, 'rb') as f:
            for event in probfile.parse_file(f):
                local_bag.add(FileItem(event.path, event.checksum))

        self.remote.send('waiting_for_files')

        remote_bag = set()
        while True:
            msg, payload = self.remote.recv()
            if msg == 'done':
                break

            assert msg == 'file_meta'

            remote_bag.add(FileItem(payload['path'], payload['checksum']))

            if payload['checksum'] in self.data_pool:
                self.remote.send('continue')
                continue

            self.remote.send('data')
            with self.data_pool.store_data(payload['checksum']) as local_file:
                while True:
                    msg, payload = self.remote.recv()
                    if msg == 'file_end':
                        break

                    assert msg == 'file_chunk'
                    local_file.write(payload)

#        self.remote.send('debug', {'on_server': local_bag - remote_bag,
#                                   'on_client': remote_bag - local_bag})

        current_version = n
        self.remote.send('sync_complete', current_version)


def main():
    assert len(sys.argv) == 2

    root_path = path.join(sys.argv[1], 'sandbox/var/repo')
    remote = picklemsg.Remote(sys.stdin, sys.stdout)

    Server(root_path, remote).loop()
