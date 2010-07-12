import os
from os import path
from subprocess import Popen, PIPE
import logging
from time import time
from contextlib import contextmanager

import argparse

import picklemsg
from checksum import repo_file_events

log = logging.getLogger('magicfolder.client')

class ClientRepo(object):
    def __init__(self, root_path):
        self.root_path = root_path
        with open(path.join(self.root_path, '.mf', 'last_sync'), 'rb') as f:
            self.last_sync = int(f.read().strip())

    def update_last_sync(self, new_value):
        self.last_sync = new_value
        with open(path.join(self.root_path, '.mf', 'last_sync'), 'wb') as f:
            f.write("%d\n" % new_value)

    @contextmanager
    def connect_to_remote(self):
        with open(path.join(self.root_path, '.mf', 'remote'), 'rb') as f:
            remote_url = f.read().strip()

        yield pipe_to_remote(remote_url)

    def send_local_status(self, remote, use_cache):
        log.debug("sync session, last_version=%d - listing local files",
                  self.last_sync)

        bytes_count = 0
        files_count = 0
        time0 = time()

        for file_item in repo_file_events(self.root_path, use_cache):
            files_count += 1
            if files_count % 100 == 0:
                log.debug("still listing local files, %d so far", files_count)

            file_meta = {
                'path': file_item.path,
                'checksum': file_item.checksum,
                'size': file_item.size,
            }
            remote.send('file_meta', file_meta)
            msg, payload = remote.recv()

            if msg == 'continue':
                continue

            assert msg == 'data'
            file_path = path.join(self.root_path, file_item.path)
            with open(file_path, 'rb') as data_file:
                remote.send_file(data_file)
                bytes_count += data_file.tell()

        log.debug("finished sending files to server (%d bytes in %d seconds)",
                  bytes_count, int(time() - time0))
        log.debug("we have %d files locally", files_count)

        remote.send('done')

    def receive_remote_update(self, remote):
        while True:
            msg, payload = remote.recv()
            if msg == 'sync_complete':
                break

            elif msg == 'file_begin':
                log.debug("new file %r", payload['path'])
                file_path = path.join(self.root_path, payload['path'])
                folder_path = path.dirname(file_path)
                if not path.isdir(folder_path):
                    os.makedirs(folder_path)

                with open(file_path, 'wb') as local_file:
                    remote.recv_file(local_file)

            elif msg == 'file_remove':
                log.debug("removing file %r", payload)
                os.unlink(path.join(self.root_path, payload))

            else:
                assert False, 'unexpected message %r' % msg

        assert payload >= self.last_sync
        self.update_last_sync(payload)
        log.debug("sync complete, now at version %d", payload)

    def sync_with_remote(self, use_cache=False):
        with self.connect_to_remote() as remote:
            remote.send('merge', self.last_sync)
            msg, payload = remote.recv()
            assert msg == 'waiting_for_files'

            self.send_local_status(remote, use_cache)

            self.receive_remote_update(remote)

            remote.send('quit')
            assert remote.recv()[0] == 'bye'

def client_sync(root_path, use_cache=False):
    ClientRepo(root_path).sync_with_remote(use_cache)

def pipe_to_remote(remote_spec):
    hostname, remote_path = remote_spec.split(':')
    child_args = ['ssh', hostname, 'mf-server', remote_path]
    p = Popen(child_args, bufsize=4096, stdin=PIPE, stdout=PIPE, stderr=PIPE)
    return picklemsg.Remote(p.stdout, p.stdin)

def parse_args():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest='subcmd')

    sync_parser = subparsers.add_parser('sync',
        help="synchronize with server")
    sync_parser.add_argument("-t", "--trust",
        action="store_true", dest="use_cache", default=False,
        help="only check timestamp and size")

    args = parser.parse_args()
    assert args.subcmd == 'sync'
    return args

def main():
    logging.basicConfig(level=logging.DEBUG)

    args = parse_args()
    root_path = os.getcwd()
    client_sync(root_path, use_cache=args.use_cache)
