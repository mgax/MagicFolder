import sys
import os
from os import path
import traceback
from collections import namedtuple
from StringIO import StringIO
import operator

import picklemsg
from probity import probfile
from probity.backup import Backup
from probity.events import FileEvent

FileItem = namedtuple('FileItem', 'path checksum size')

def event_to_fileitem(event):
    return FileItem(event.path, event.checksum, event.size)

def fileitem_to_event(fileitem):
    return FileEvent('_', fileitem.path, fileitem.checksum, fileitem.size)

def dump_fileitems(file_ob, fileitem_bag):
    with probfile.YamlDumper(file_ob) as yaml_dumper:
        for fileitem in sorted(fileitem_bag, key=operator.attrgetter('path')):
            yaml_dumper.write(fileitem_to_event(fileitem))

def object_path(root_path, checksum):
    h1, h2 = checksum[:2], checksum[2:]
    return path.join(root_path, 'objects', h1, h2)

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

        self.remote.send('version_number', n)
        with self.open_version_index(n, 'rb') as f:
            for event in probfile.parse_file(f):
                file_meta = {
                    'path': event.path,
                    'checksum': event.checksum,
                    'size': event.size,
                }
                self.remote.send('file_begin', file_meta)
                with open(object_path(self.root_path,
                                      event.checksum), 'rb') as f:
                    self.remote.send_file(f)

        self.remote.send('done')

    def open_version_index(self, n, mode):
        return open(path.join(self.root_path, 'versions/%d' % n), mode)

    def msg_merge(self, payload):
        versions_path = path.join(self.root_path, 'versions')
        latest_version = max(int(v) for v in os.listdir(versions_path))
        remote_base_version = payload

        current_server_bag = set()
        with self.open_version_index(latest_version, 'rb') as f:
            for event in probfile.parse_file(f):
                current_server_bag.add(event_to_fileitem(event))

        if remote_base_version == latest_version:
            remote_outdated = False
            old_server_bag = current_server_bag
        else:
            remote_outdated = True
            old_server_bag = set()
            with self.open_version_index(remote_base_version, 'rb') as f:
                for event in probfile.parse_file(f):
                    old_server_bag.add(event_to_fileitem(event))

        self.remote.send('waiting_for_files')

        temp_version_file = StringIO()
        client_bag = set()

        while True:
            msg, payload = self.remote.recv()
            if msg == 'done':
                break

            assert msg == 'file_meta'

            checksum = payload['checksum']
            client_bag.add(FileItem(payload['path'], checksum,
                                    payload['size']))

            if checksum in self.data_pool:
                self.remote.send('continue')
                continue

            self.remote.send('data')
            with self.data_pool.store_data(checksum) as local_file:
                self.remote.recv_file(local_file)

        if remote_outdated:
            assert old_server_bag == client_bag
            current_version = latest_version

            for new_file in current_server_bag - client_bag:
                event = fileitem_to_event(new_file)
                file_meta = {
                    'path': event.path,
                    'checksum': event.checksum,
                    'size': event.size,
                }
                self.remote.send('file_begin', file_meta)
                with open(object_path(self.root_path,
                                      event.checksum), 'rb') as f:
                    self.remote.send_file(f)

            for removed_file in client_bag - current_server_bag:
                self.remote.send('file_remove', removed_file.path)

        else:
            if current_server_bag == client_bag:
                current_version = latest_version
            else:
                current_version = latest_version + 1
                with self.open_version_index(current_version, 'wb') as f:
                    dump_fileitems(f, client_bag)

        self.remote.send('sync_complete', current_version)


def main():
    assert len(sys.argv) == 2

    root_path = path.join(sys.argv[1], 'sandbox/var/repo')
    remote = picklemsg.Remote(sys.stdin, sys.stdout)

    Server(root_path, remote).loop()
