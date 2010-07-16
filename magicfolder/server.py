import sys
import os
from os import path
import traceback
from StringIO import StringIO
import operator
import logging
from contextlib import contextmanager

import picklemsg
from blobdb import BlobDB
from checksum import FileItem, read_version_file, write_version_file

log = logging.getLogger('magicfolder.server')

def dump_fileitems(fh, bag):
    with write_version_file(fh) as write_file_item:
        for i in sorted(bag, key=operator.attrgetter('path')):
            write_file_item(i)

def server_init(root_path):
    os.mkdir(path.join(root_path, 'objects'))
    os.mkdir(path.join(root_path, 'versions'))
    with open(path.join(root_path, 'versions', '0'), 'wb') as f:
        pass

def server_sync(root_path, remote):
    assert path.isdir(root_path)
    data_pool = BlobDB(path.join(root_path, 'objects'))

    def open_version_index(n, mode):
        return open(path.join(root_path, 'versions/%d' % n), mode)

    msg, payload = remote.recv()
    assert msg == 'sync'

    versions_path = path.join(root_path, 'versions')
    latest_version = max(int(v) for v in os.listdir(versions_path))
    remote_base_version = payload

    log.debug("Begin sync at version %d, client last_sync is %d",
              latest_version, remote_base_version)

    with open_version_index(latest_version, 'rb') as f:
        current_server_bag = set(read_version_file(f))

    if remote_base_version == latest_version:
        remote_outdated = False
        old_server_bag = current_server_bag
    else:
        remote_outdated = True
        old_server_bag = set()
        if remote_base_version != 0:
            with open_version_index(remote_base_version, 'rb') as f:
                old_server_bag.update(read_version_file(f))

    remote.send('waiting_for_files')

    temp_version_file = StringIO()
    client_bag = set()

    while True:
        msg, payload = remote.recv()
        if msg == 'done':
            break

        assert msg == 'file_meta'

        client_bag.add(FileItem(payload['path'], payload['checksum'],
                                payload['size'], None))

    for i in client_bag:
        if i.checksum not in data_pool:
            log.debug("Downloading data for %s (size: %r, path: %r)",
                      i.checksum, i.size, i.path)
            remote.send('data', i.checksum)
            with data_pool.write_file(i.checksum) as bf:
                remote.recv_file(bf)

    if remote_outdated:
        log.debug("Client was at old version, performing merge")
        for file_item in old_server_bag - client_bag:
            log.debug("Removed by client: %r", file_item)
        for file_item in client_bag - old_server_bag:
            log.debug("Added by client: %r", file_item)
        assert old_server_bag == client_bag
        current_version = latest_version

        for new_file in current_server_bag - client_bag:
            file_meta = {
                'path': new_file.path,
                'checksum': new_file.checksum,
                'size': new_file.size,
            }
            log.debug("Sending file %s for path %r",
                      new_file.checksum, new_file.path)
            remote.send('file_begin', file_meta)
            with data_pool.read_file(new_file.checksum) as f:
                remote.send_file(f)

        for removed_file in client_bag - current_server_bag:
            assert removed_file.checksum in data_pool
            log.debug("Asking client to remove %s (size: %r, path: %r)",
                      removed_file.checksum, removed_file.size,
                      removed_file.path)
            remote.send('file_remove', removed_file.path)

    else:
        if current_server_bag == client_bag:
            current_version = latest_version
            log.debug("Client has no changes, staying at version %d",
                      current_version)
        else:
            current_version = latest_version + 1
            log.debug("Client has changes, creating new version %d",
                      current_version)
            with open_version_index(current_version, 'wb') as f:
                dump_fileitems(f, client_bag)

    log.debug("Sync complete")
    remote.send('sync_complete', current_version)

    msg, payload = remote.recv()
    assert msg == 'quit'
    remote.send('bye')


@contextmanager
def try_except_send_remote(remote):
    try:
        yield
    except:
        log.exception("Exception while performing sync")
        try:
            error_report = traceback.format_exc()
        except:
            error_report = "[exception while formatting traceback]"
        remote.send('error', error_report)

def main():
    assert len(sys.argv) == 2
    root_path = path.join(sys.argv[1])

    logging.basicConfig(level=logging.DEBUG,
                        filename=path.join(root_path, 'debug.log'))
    remote = picklemsg.Remote(sys.stdin, sys.stdout)

    with try_except_send_remote(remote):
        server_sync(root_path, remote)
