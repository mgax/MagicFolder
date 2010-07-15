import sys
import os
from os import path
import traceback
from StringIO import StringIO
import operator
import logging
from contextlib import contextmanager

from probity import probfile
from probity.backup import Backup
from probity.events import FileEvent

import picklemsg
from checksum import FileItem

log = logging.getLogger('magicfolder.server')

def event_to_fileitem(event):
    return FileItem(event.path, event.checksum, event.size, 0)

def fileitem_to_event(fileitem):
    return FileEvent('_', fileitem.path, fileitem.checksum, fileitem.size)

def dump_fileitems(file_ob, fileitem_bag):
    with probfile.YamlDumper(file_ob) as yaml_dumper:
        for fileitem in sorted(fileitem_bag, key=operator.attrgetter('path')):
            yaml_dumper.write(fileitem_to_event(fileitem))

def object_path(root_path, checksum):
    h1, h2 = checksum[:2], checksum[2:]
    return path.join(root_path, 'objects', h1, h2)

def server_init(root_path):
    os.mkdir(path.join(root_path, 'objects'))
    os.mkdir(path.join(root_path, 'versions'))
    with open(path.join(root_path, 'versions', '0'), 'wb') as f:
        pass

def server_sync(root_path, remote):
    assert path.isdir(root_path)
    data_pool = Backup(path.join(root_path, 'objects'))

    def open_version_index(n, mode):
        return open(path.join(root_path, 'versions/%d' % n), mode)

    msg, payload = remote.recv()
    assert msg == 'merge'

    versions_path = path.join(root_path, 'versions')
    latest_version = max(int(v) for v in os.listdir(versions_path))
    remote_base_version = payload

    log.debug("Begin sync at version %d, remote last_sync is %d",
              latest_version, remote_base_version)

    current_server_bag = set()
    with open_version_index(latest_version, 'rb') as f:
        for event in probfile.parse_file(f):
            current_server_bag.add(event_to_fileitem(event))

    if remote_base_version == latest_version:
        remote_outdated = False
        old_server_bag = current_server_bag
    else:
        remote_outdated = True
        old_server_bag = set()
        if remote_base_version != 0:
            with open_version_index(remote_base_version, 'rb') as f:
                for event in probfile.parse_file(f):
                    old_server_bag.add(event_to_fileitem(event))

    remote.send('waiting_for_files')

    temp_version_file = StringIO()
    client_bag = set()

    while True:
        msg, payload = remote.recv()
        if msg == 'done':
            break

        assert msg == 'file_meta'

        client_bag.add(FileItem(payload['path'], payload['checksum'],
                                payload['size'], 0))

        if payload['checksum'] in data_pool:
            remote.send('continue')
            continue

        log.debug("Downloading data for %s (size: %r, path: %r)",
                  payload['checksum'], payload['size'], payload['path'])
        remote.send('data')
        with data_pool.store_data(payload['checksum']) as local_file:
            remote.recv_file(local_file)

    if remote_outdated:
        log.debug("Client was at old version, performing merge")
        assert old_server_bag == client_bag
        current_version = latest_version

        for new_file in current_server_bag - client_bag:
            event = fileitem_to_event(new_file)
            file_meta = {
                'path': event.path,
                'checksum': event.checksum,
                'size': event.size,
            }
            log.debug("Sending file %s for path %r",
                      new_file.checksum, new_file.path)
            remote.send('file_begin', file_meta)
            with open(object_path(root_path, event.checksum), 'rb') as f:
                remote.send_file(f)

        for removed_file in client_bag - current_server_bag:
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
