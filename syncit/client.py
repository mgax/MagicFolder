import picklemsg

def do_sync(root_path, remote):
    remote.send("ping")
    print remote.recv()
    remote.send("quit")
    print remote.recv()

def main():
    import sys
    assert len(sys.argv) == 3
    do_sync(sys.argv[1], picklemsg.pipe_to_remote(sys.argv[2]))
