import picklemsg

def do_sync(root_path, remote):
    print 'wow, got this far!'
    remote.send("gigel")
    print remote.recv()

def main():
    import sys
    assert len(sys.argv) == 3
    do_sync(sys.argv[1], picklemsg.pipe_to_remote(sys.argv[2]))
