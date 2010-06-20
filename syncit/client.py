import os
from os import path

import picklemsg

def receive_full_version(root_path, remote):
    remote.send('stream_latest_version')
    print "saving latest version to %r" % root_path

    msg, payload = remote.recv()
    assert msg == 'version_number'
    print 'version %d' % payload

    while True:
        msg, payload = remote.recv()
        if msg == 'done':
            break

        assert msg == 'file_begin'
        print payload['path']

        file_path = path.join(root_path, payload['path'])
        folder_path = path.dirname(file_path)
        if not path.isdir(folder_path):
            os.makedirs(folder_path)

        with open(file_path, 'wb') as local_file:
            while True:
                msg, payload = remote.recv()
                if msg == 'file_end':
                    break

                assert msg == 'file_chunk'
                local_file.write(payload)

def do_sync(root_path, remote):
    receive_full_version(root_path, remote)
    remote.send('quit')
    assert remote.recv()[0] == 'bye'

def main():
    import sys
    assert len(sys.argv) == 3
    do_sync(sys.argv[1], picklemsg.pipe_to_remote(sys.argv[2]))
