import os
from os import path
from subprocess import Popen, PIPE
import logging
from time import time
from contextlib import contextmanager

import argparse

import picklemsg
from checksum import FileItem, repo_file_events
from uilib import ColorfulUi, DummyUi, pretty_bytes

UI_UPDATE_TIME = 0.5 # half a second

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

    def send_local_status(self, remote, ui, use_cache):
        log.debug("Sync session, last_sync %r", self.last_sync)

        with ui.status_line() as print_line:
            t0 = time()
            file_item_map = {}
            n = 0
            print_line("Reading local files...")

            for i in repo_file_events(self.root_path, use_cache):
                i_for_server = FileItem(i.path, i.checksum, i.size, None)
                remote.send('file_meta', i_for_server)
                file_item_map[i.checksum] = i

                n += 1
                if time() - t0 > UI_UPDATE_TIME:
                    t0 = time()
                    print_line("Reading local files... %d" % n)
        ui.out("Reading local files... %d done\n" % n)

        log.debug("Finished sending index to server")
        remote.send('done')
        return file_item_map

    def _send_file(self, file_item, remote):
        log.debug("uploading file %s, path %r",
                  file_item.checksum, file_item.path)

        file_path = path.join(self.root_path, file_item.path)
        with open(file_path, 'rb') as data_file:
            remote.send_file(data_file)

    def _recv_file(self, file_item, remote):
        log.debug("Receiving file %r %r", file_item.path, file_item.checksum)

        file_path = path.join(self.root_path, file_item.path)

        folder_path = path.dirname(file_path)
        if not path.isdir(folder_path):
            os.makedirs(folder_path)

        with open(file_path, 'wb') as local_file:
            remote.recv_file(local_file)

    def _remove_file(self, file_item):
        log.debug("Removing file %r", file_item.path)
        os.unlink(path.join(self.root_path, file_item.path))

    def receive_remote_update(self, remote, ui, file_item_map):
        t0 = time()
        bytes_up = bytes_down = 0
        files_new = set(); files_del = set()
        def bytes_msg():
            return ("Transferring... up: %s, down: %s"
                    % (pretty_bytes(bytes_up),
                       pretty_bytes(bytes_down)))

        with ui.status_line() as print_line:
            print_line(bytes_msg())

            while True:
                msg, payload = remote.recv()
                if msg == 'sync_complete':
                    break

                elif msg == 'data':
                    file_item = file_item_map[payload]
                    self._send_file(file_item, remote)
                    bytes_up += file_item.size

                elif msg == 'file_begin':
                    self._recv_file(payload, remote)
                    bytes_down += payload.size
                    files_new.add(payload)

                elif msg == 'file_remove':
                    self._remove_file(payload)
                    files_del.add(payload)

                else:
                    assert False, 'unexpected message %r' % msg

                if time() - t0 > UI_UPDATE_TIME:
                    t0 = time()
                    print_line(bytes_msg())

        ui.out(bytes_msg() + "\n")

        assert payload >= self.last_sync
        self.update_last_sync(payload)
        log.debug("Sync complete, now at version %d", payload)

        def print_files_colored(files, color):
            for i in sorted(files):
                with ui.colored(color) as color_print:
                    color_print(i.path)
                ui.out(' %s\n' % pretty_bytes(i.size))

        print_files_colored(files_del, 'red')
        print_files_colored(files_new, 'green')
        ui.out("At version %d\n" % payload)

    def sync_with_remote(self, ui=DummyUi(), use_cache=False):
        with self.connect_to_remote() as remote:
            remote.send('sync', self.last_sync)
            msg, payload = remote.recv()
            assert msg == 'waiting_for_files'

            file_item_map = self.send_local_status(remote, ui, use_cache)

            self.receive_remote_update(remote, ui, file_item_map)

            remote.send('quit')
            assert remote.recv()[0] == 'bye'

def pipe_to_remote(remote_spec):
    hostname, remote_path = remote_spec.split(':')
    child_args = ['ssh', hostname, 'mf-server', remote_path]
    log.debug("running %r", child_args)
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
    sync_parser.add_argument("-p", "--paranoid",
        action="store_false", dest="use_cache", default=True,
        help="don't trust timestamp and size, always calculate checksum")

    args = parser.parse_args()
    return args

def main():
    args = parse_args()
    root_path = os.getcwd()

    if args.subcmd == 'init':
        if args.server:
            from server import server_init
            server_init(root_path)
        else:
            assert len(args.remote) == 1
            client_init(root_path, args.remote[0])
    elif args.subcmd == 'sync':
        logging.basicConfig(level=logging.DEBUG,
                            filename=path.join(root_path, '.mf', 'debug.log'))

        try:
            ClientRepo(root_path).sync_with_remote(use_cache=args.use_cache,
                                                   ui=ColorfulUi())
        except:
            log.exception("Exception while performing sync")
            raise
    else:
        raise ValueError('bad param')
