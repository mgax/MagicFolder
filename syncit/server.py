import picklemsg

def do_server(root_path, remote):
    name = remote.recv()
    remote.send("server at %r talking to %r" % (root_path, name))

def main():
    import sys
    assert len(sys.argv) == 2
    do_server(sys.argv[1], picklemsg.Remote(sys.stdin, sys.stdout))
