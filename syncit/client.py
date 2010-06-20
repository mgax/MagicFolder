import os
from os import path
from subprocess import Popen, PIPE

import picklemsg
from probity.walk import walk_path as probity_walk_path

class Client(object):
    def __init__(self, root_path, remote):
        self.root_path = root_path
        self.private_path = path.join(root_path, '.syncit')
        self.remote = remote

    def sync(self):
        if path.isdir(self.root_path):
            assert path.isdir(self.private_path)
            self.merge_versions()
        else:
            os.makedirs(self.private_path)
            self.receive_full_version()

        self.remote.send('quit')
        assert self.remote.recv()[0] == 'bye'

    def receive_full_version(self):
        self.remote.send('stream_latest_version')
        print "saving latest version to %r" % self.root_path

        msg, payload = self.remote.recv()
        assert msg == 'version_number'
        with open(path.join(self.private_path, 'last_sync'), 'wb') as f:
            f.write("%d\n" % payload)

        while True:
            msg, payload = self.remote.recv()
            if msg == 'done':
                break

            assert msg == 'file_begin'
            print payload['path']

            file_path = path.join(self.root_path, payload['path'])
            folder_path = path.dirname(file_path)
            if not path.isdir(folder_path):
                os.makedirs(folder_path)

            with open(file_path, 'wb') as local_file:
                while True:
                    msg, payload = self.remote.recv()
                    if msg == 'file_end':
                        break

                    assert msg == 'file_chunk'
                    local_file.write(payload)

    def merge_versions(self):
        raise NotImplementedError


def pipe_to_remote(remote_spec):
    hostname, remote_path = remote_spec.split(':')
    script_path = path.join(remote_path, 'sandbox/bin/syncserver')
    child_args = ['ssh', hostname, script_path, remote_path]
    p = Popen(child_args, bufsize=4096, stdin=PIPE, stdout=PIPE, stderr=PIPE)
    return picklemsg.Remote(p.stdout, p.stdin)

def main():
    import sys
    assert len(sys.argv) == 3

    root_path = sys.argv[1]
    remote = pipe_to_remote(sys.argv[2])

    Client(root_path, remote).sync()
