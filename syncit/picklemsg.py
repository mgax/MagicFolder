from subprocess import Popen, PIPE
import cPickle as pickle
from os import path

class Remote(object):
    def __init__(self, in_file, out_file):
        self.in_unpickler = pickle.Unpickler(in_file)
        self.out_pickler = pickle.Pickler(out_file, 2) # protocol version 2
        self.out_file = out_file

    def send(self, msg, payload=None):
        self.out_pickler.dump( (msg, payload) )
        self.out_file.flush()
        self.out_pickler.clear_memo()

    def recv(self):
        return self.in_unpickler.load()

    def __iter__(self):
        return self

    def next(self):
        return self.recv()

def pipe_to_remote(remote_spec):
    hostname, remote_path = remote_spec.split(':')
    script_path = path.join(remote_path, 'sandbox/bin/syncserver')
    child_args = ['ssh', hostname, script_path, remote_path]
    p = Popen(child_args, bufsize=4096, stdin=PIPE, stdout=PIPE, stderr=PIPE)
    return Remote(p.stdout, p.stdin)
