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

def cooldown(interval):
    from time import time
    from functools import wraps
    def decorator(f):
        class state(object): t0 = 0
        @wraps(f)
        def wrapper(*args, **kwargs):
            t = time()
            if t < state.t0 + interval:
                return
            state.t0 = t
            f(*args, **kwargs)
        return wrapper
    return decorator

def client_init(root_path, remote_url):
    os.mkdir(path.join(root_path, '.mf'))
    with open(path.join(root_path, '.mf', 'remote'), 'wb') as f:
        f.write("%s\n" % remote_url)
    with open(path.join(root_path, '.mf', 'last_sync'), 'wb') as f:
        f.write("0\n")

class WorkingTree(object):
    def __init__(self, root_path):
        self.root_path = root_path
        with open(path.join(self.root_path, '.mf', 'last_sync'), 'rb') as f:
            self.last_sync = int(f.read().strip())

    def update_last_sync(self, new_value):
        self.last_sync = new_value
        with open(path.join(self.root_path, '.mf', 'last_sync'), 'wb') as f:
            f.write("%d\n" % new_value)

    def _get_remote_url(self):
        with open(path.join(self.root_path, '.mf', 'remote'), 'rb') as f:
            return f.read().strip()

    def iter_files(self, use_cache):
        return repo_file_events(self.root_path, use_cache)

    def open_read(self, file_item):
        file_path = path.join(self.root_path, file_item.path)
        return open(file_path, 'rb')

    def open_write(self, file_item):
        file_path = path.join(self.root_path, file_item.path)
        folder_path = path.dirname(file_path)
        if not path.isdir(folder_path):
            os.makedirs(folder_path)
        return open(file_path, 'wb')

    def remove_file(self, file_item):
        os.unlink(path.join(self.root_path, file_item.path))
        folder = path.dirname(file_item.path)

        while folder:
            folder_abs = path.join(self.root_path, folder)
            if os.listdir(folder_abs):
                break
            os.rmdir(folder_abs)
            folder = path.dirname(folder)

class SyncClient(object):
    def __init__(self, working_tree, remote, ui=DummyUi()):
        self.wt = working_tree
        self.remote = remote
        self.ui = ui

    def update_last_sync(self, new_value):
        self.wt.update_last_sync(new_value)

    @contextmanager
    def connect_to_remote(self, remote_url):
        log.debug("Connecting to server %r", remote_url)
        yield pipe_to_remote(remote_url)

    def send_local_status(self, use_cache):
        log.debug("Sync session, last_sync %r", self.wt.last_sync)

        @cooldown(UI_UPDATE_TIME)
        def update_files_ui(n):
            print_line("Reading local files... %d" % n)

        with self.ui.status_line() as print_line:
            file_item_map = {}
            n = 0
            print_line("Reading local files...")

            for i in self.wt.iter_files(use_cache):
                i_for_server = FileItem(i.path, i.checksum, i.size, None)
                self.remote.send('file_meta', i_for_server)
                file_item_map[i.checksum] = i

                n += 1
                update_files_ui(n)
        self.ui.out("Reading local files... %d done\n" % n)

        log.debug("Finished sending index to server")
        self.remote.send('done')
        return file_item_map

    def receive_remote_update(self, file_item_map):
        # TODO the local variables should be instance variables, and
        # this function needs to be split into many smaller ones.
        bytes_count = {'up': 0, 'down': 0}
        files_new = set(); files_del = set()
        def bytes_msg():
            return ("Transferring... up: %s, down: %s"
                    % (pretty_bytes(bytes_count['up']),
                       pretty_bytes(bytes_count['down'])))

        with self.ui.status_line() as print_line:
            print_line(bytes_msg())

            @cooldown(UI_UPDATE_TIME)
            def update_bytes_ui():
                print_line(bytes_msg())

            def progress_up(n_bytes):
                bytes_count['up'] += n_bytes
                update_bytes_ui()

            def progress_down(n_bytes):
                bytes_count['down'] += n_bytes
                update_bytes_ui()

            while True:
                msg, payload = self.remote.recv()
                if msg == 'sync_complete':
                    break

                elif msg == 'data':
                    file_item = file_item_map[payload]
                    log.debug("uploading file %s, path %r",
                              file_item.checksum, file_item.path)
                    with self.wt.open_read(file_item) as data_file:
                        self.remote.send_file(data_file, progress_up)

                elif msg == 'file_begin':
                    file_item = payload
                    log.debug("Receiving file %r %r",
                              file_item.path, file_item.checksum)
                    with self.wt.open_write(file_item) as local_file:
                        self.remote.recv_file(local_file, progress_down)
                    files_new.add(payload)

                elif msg == 'file_remove':
                    file_item = payload
                    log.debug("Removing file %r", file_item.path)
                    self.wt.remove_file(file_item)
                    files_del.add(payload)

                else:
                    assert False, 'unexpected message %r' % msg

        self.ui.out(bytes_msg() + "\n")

        assert payload >= self.wt.last_sync
        self.update_last_sync(payload)
        log.debug("Sync complete, now at version %d", payload)

        msg, diff = self.remote.recv()
        assert msg == 'commit_diff'

        def print_files_colored(files, color, size=False):
            for i in sorted(files):
                with self.ui.colored(color) as color_print:
                    color_print(' ' + i.path)
                if size:
                    self.ui.out(' %s' % pretty_bytes(i.size))
                self.ui.out('\n')

        self.ui.out("Saving changes to server ...\n")
        print_files_colored(diff['removed'], 'red')
        print_files_colored(diff['added'], 'green', size=True)
        self.ui.out("Updating local copy ...\n")
        print_files_colored(files_del, 'red')
        print_files_colored(files_new, 'green', size=True)
        self.ui.out("At version %d\n" % payload)

    def sync_with_remote(self, use_cache=False):
        self.remote.send('sync', self.wt.last_sync)
        msg, payload = self.remote.recv()
        assert msg == 'waiting_for_files'

        file_item_map = self.send_local_status(use_cache)

        self.receive_remote_update(file_item_map)

        self.remote.send('quit')
        assert self.remote.recv()[0] == 'bye'

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
            wt = WorkingTree(root_path)
            remote = pipe_to_remote(wt._get_remote_url())
            ui = ColorfulUi()
            session = SyncClient(wt, remote, ui)
            session.sync_with_remote(use_cache=args.use_cache)
        except:
            log.exception("Exception while performing sync")
            raise
    else:
        raise ValueError('bad param')
