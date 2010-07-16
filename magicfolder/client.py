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

def client_init(root_path, remote_url):
    os.mkdir(path.join(root_path, '.mf'))
    with open(path.join(root_path, '.mf', 'remote'), 'wb') as f:
        f.write("%s\n" % remote_url)
    with open(path.join(root_path, '.mf', 'last_sync'), 'wb') as f:
        f.write("0\n")

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

        log.debug("Connecting to server %r", remote_url)

        yield pipe_to_remote(remote_url)

    def send_local_status(self, remote, use_cache):
        log.debug("Sync session, last_sync %r", self.last_sync)

        file_item_map = {}
        for file_item in repo_file_events(self.root_path, use_cache):
            file_meta = {
                'path': file_item.path,
                'checksum': file_item.checksum,
                'size': file_item.size,
            }
            remote.send('file_meta', file_meta)
            file_item_map[file_item.checksum] = file_item

        log.debug("Finished sending index to server")
        remote.send('done')
        return file_item_map

    def receive_remote_update(self, remote, file_item_map):
        while True:
            msg, payload = remote.recv()
            if msg == 'sync_complete':
                break

            elif msg == 'data':
                file_item = file_item_map[payload]
                file_path = path.join(self.root_path, file_item.path)
                log.debug("uploading file %s, path %r",
                          file_item.checksum, file_item.path)
                with open(file_path, 'rb') as data_file:
                    remote.send_file(data_file)

            elif msg == 'file_begin':
                log.debug("Receiving file %r %r",
                          payload['path'], payload['checksum'])
                file_path = path.join(self.root_path, payload['path'])
                folder_path = path.dirname(file_path)
                if not path.isdir(folder_path):
                    os.makedirs(folder_path)

                with open(file_path, 'wb') as local_file:
                    remote.recv_file(local_file)

            elif msg == 'file_remove':
                log.debug("Removing file %r", payload)
                os.unlink(path.join(self.root_path, payload))

            else:
                assert False, 'unexpected message %r' % msg

        assert payload >= self.last_sync
        self.update_last_sync(payload)
        log.debug("Sync complete, now at version %d", payload)

    def sync_with_remote(self, use_cache=False):
        with self.connect_to_remote() as remote:
            remote.send('sync', self.last_sync)
            msg, payload = remote.recv()
            assert msg == 'waiting_for_files'

            file_item_map = self.send_local_status(remote, use_cache)

            self.receive_remote_update(remote, file_item_map)

            remote.send('quit')
            assert remote.recv()[0] == 'bye'

def pipe_to_remote(remote_spec):
    hostname, remote_path = remote_spec.split(':')
    child_args = ['ssh', hostname, 'mf-server', remote_path]
    p = Popen(child_args, bufsize=4096, stdin=PIPE, stdout=PIPE, stderr=PIPE)
    return picklemsg.Remote(p.stdout, p.stdin)

def parse_args():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest='subcmd')

    init_parser = subparsers.add_parser('init',
        help="initialize repository")
    init_parser.add_argument("remote", nargs='*',
        help="remote server url")
    init_parser.add_argument("-s", "--server",
        action="store_true", dest="server", default=False,
        help="initialize a server repository")

    sync_parser = subparsers.add_parser('sync',
        help="synchronize with server")
    sync_parser.add_argument("-t", "--trust",
        action="store_true", dest="use_cache", default=False,
        help="only check timestamp and size")

    args = parser.parse_args()
    return args

def main():
    args = parse_args()
    root_path = os.getcwd()

    logging.basicConfig(level=logging.DEBUG,
                        filename=path.join(root_path, '.mf', 'debug.log'))

    if args.subcmd == 'init':
        if args.server:
            from server import server_init
            server_init(root_path)
        else:
            assert len(args.remote) == 1
            client_init(root_path, args.remote[0])
    elif args.subcmd == 'sync':
        try:
            ClientRepo(root_path).sync_with_remote(use_cache=args.use_cache)
        except:
            log.exception("Exception while performing sync")
    else:
        raise ValueError('bad param')
