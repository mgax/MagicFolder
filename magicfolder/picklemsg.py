import cPickle as pickle

CHUNK_SIZE = 64 * 1024 # 64 KB

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
        msg, payload = self.in_unpickler.load()
        if msg == 'error':
            print "error from remote endpoint\n%s" % payload
        return msg, payload

    def send_file(self, src_file):
        while True:
            chunk = src_file.read(CHUNK_SIZE)
            if not chunk:
                break
            self.send('file_chunk', chunk)

        self.send('file_end')

    def recv_file(self, dst_file):
        while True:
            msg, payload = self.recv()
            if msg == 'file_end':
                break

            assert msg == 'file_chunk'
            dst_file.write(payload)

    def __iter__(self):
        return self

    def next(self):
        return self.recv()
