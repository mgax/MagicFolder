import cPickle as pickle

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
