import os
from os import path
from subprocess import Popen, PIPE
import logging
from time import time

import picklemsg
from probity.walk import walk_path as probity_walk_path

log = logging.getLogger('magicfolder.client')

def client_sync(root_path, remote):
    private_path = path.join(root_path, '.mf')

    last_sync_path = path.join(private_path, 'last_sync')

    if path.isdir(root_path):
        with open(last_sync_path, 'rb') as f:
            last_version = int(f.read().strip())
    else:
        last_version = 0
        os.makedirs(private_path)

    log.debug("sync session, last_version=%d - listing local files",
              last_version)

    remote.send('merge', last_version)
    msg, payload = remote.recv()
    assert msg == 'waiting_for_files'

    bytes_count = 0
    files_count = 0
    time0 = time()

    root_name = path.basename(root_path)
    for event in probity_walk_path(root_path):
        file_path = event.path.split('/', 1)[1]
        if file_path.startswith('.mf/'):
            continue

        files_count += 1
        if files_count % 100 == 0:
            log.debug("still listing local files, %d so far", files_count)

        file_meta = {
            'path': file_path,
            'checksum': event.checksum,
            'size': event.size,
        }
        remote.send('file_meta', file_meta)
        msg, payload = remote.recv()

        if msg == 'continue':
            continue

        assert msg == 'data'
        #print 'sending data for %r' % file_path
        with open(event.fs_path, 'rb') as data_file:
            remote.send_file(data_file)
            bytes_count += data_file.tell()

    log.debug("finished sending files to server (%d bytes in %d seconds)",
              bytes_count, int(time() - time0))
    log.debug("we have %d files locally", files_count)

    remote.send('done')

    while True:
        msg, payload = remote.recv()
        if msg == 'sync_complete':
            break

        elif msg == 'file_begin':
            log.debug("new file %r", payload['path'])
            file_path = path.join(root_path, payload['path'])
            folder_path = path.dirname(file_path)
            if not path.isdir(folder_path):
                os.makedirs(folder_path)

            with open(file_path, 'wb') as local_file:
                remote.recv_file(local_file)

        elif msg == 'file_remove':
            log.debug("removing file %r", payload)
            os.unlink(path.join(root_path, payload))

        else:
            assert False, 'unexpected message %r' % msg

    assert payload >= last_version
    with open(last_sync_path, 'wb') as f:
        f.write("%d\n" % payload)

    log.debug("sync complete, now at version %d", payload)

    remote.send('quit')
    assert remote.recv()[0] == 'bye'


def pipe_to_remote(remote_spec):
    hostname, remote_path = remote_spec.split(':')
    child_args = ['ssh', hostname, 'mf-server', remote_path]
    p = Popen(child_args, bufsize=4096, stdin=PIPE, stdout=PIPE, stderr=PIPE)
    return picklemsg.Remote(p.stdout, p.stdin)

def main():
    import sys
    assert len(sys.argv) == 2
    assert sys.argv[1] == 'sync'

    logging.basicConfig(level=logging.DEBUG)

    root_path = os.getcwd()
    with open(path.join(root_path, '.mf/remote'), 'rb') as f:
        remote_url = f.read().strip()
    remote = pipe_to_remote(remote_url)

    client_sync(root_path, remote)
