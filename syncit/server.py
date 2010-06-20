import sys
from os import path
import traceback
import picklemsg

class Server(object):
    def __init__(self, root_path, remote):
        assert path.isdir(root_path)
        self.root_path = root_path
        self.remote = remote

    def loop(self):
        for msg, payload in self.remote:
            try:
                if msg == 'quit':
                    self.remote.send("bye")
                    return

                func_name = 'msg_%s' % msg
                if hasattr(self, func_name):
                    method = getattr(self, func_name)
                    if callable(method):
                        method(payload)
                        continue

                raise ValueError("unknown message %r" % msg)

            except:
                try:
                    error_report = traceback.format_exc()
                except:
                    error_report = "[exception while formatting traceback]"
                self.remote.send("error", error_report)

    def msg_ping(self, payload):
        self.remote.send("pong", "server at %r" % self.root_path)


def main():
    assert len(sys.argv) == 2

    root_path = path.join(sys.argv[1], 'sandbox/var/repo')
    remote = picklemsg.Remote(sys.stdin, sys.stdout)

    Server(root_path, remote).loop()
