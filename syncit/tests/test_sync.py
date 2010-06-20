import unittest
import tempfile
import shutil
import os
from os import path
import threading
from Queue import Queue
from hashlib import sha1

from probity.backup import Backup

from syncit.client import client_sync
from syncit.server import server_sync, try_except_send_remote
from syncit.picklemsg import Remote

def sha1hex(s):
    return sha1(s).hexdigest()

class TestRemote(Remote):
    def __init__(self, in_queue, out_queue):
        self.in_queue = in_queue
        self.out_queue = out_queue

    def send(self, msg, payload=None):
        self.out_queue.put( (msg, payload) )

    def recv(self):
        msg, payload = self.in_queue.get()
        if msg == 'error':
            print "error from remote endpoint\n%s" % payload
        return msg, payload

def do_server_loop(root_path, in_queue, out_queue):
    remote = TestRemote(in_queue, out_queue)
    with try_except_send_remote(remote):
        server_sync(root_path, remote)

def do_client_sync(root_path, in_queue, out_queue):
    remote = TestRemote(in_queue, out_queue)
    client_sync(root_path, remote)

def do_client_server(client_root, server_root):
    c2s = Queue()
    s2c = Queue()
    server_thread = threading.Thread(target=do_server_loop,
                                     args=(server_root, c2s, s2c))
    server_thread.start()
    do_client_sync(client_root, s2c, c2s)
    server_thread.join()

class SyncTest(unittest.TestCase):
    def setUp(self):
        self.client_tmp_path = tempfile.mkdtemp()
        self.client_root = path.join(self.client_tmp_path, 'repo')
        self.server_root = tempfile.mkdtemp()
        self.server_objects_path = path.join(self.server_root, 'objects')
        self.server_versions_path = path.join(self.server_root, 'versions')
        os.mkdir(self.server_objects_path)
        os.mkdir(self.server_versions_path)

    def run_loop(self):
        do_client_server(self.client_root, self.server_root)

    def tearDown(self):
        shutil.rmtree(self.client_tmp_path)
        shutil.rmtree(self.server_root)

    def server_fixtures(self, version, files):
        server_objs = Backup(self.server_objects_path)
        index_path = path.join(self.server_versions_path, str(version))
        with open(index_path, 'wb') as index_f:
            for file_path, file_data in files.iteritems():
                index_f.write('%s: {sha1: %s, size: %d}\n' %
                              (file_path, sha1hex(file_data), len(file_data)))
                with server_objs.store_data(sha1hex(file_data)) as data_f:
                    data_f.write(file_data)

    def test_initial(self):
        self.server_fixtures(1, {
            'path_one': "hello world",
            'path_two': "hi there",
        })
        self.run_loop()

        self.assertEqual(set(os.listdir(self.client_root)),
                         set(['.syncit', 'path_one', 'path_two']))
        with open(path.join(self.client_root, '.syncit/last_sync'), 'rb') as f:
            self.assertEqual(f.read(), "1\n")
        with open(path.join(self.client_root, 'path_one'), 'rb') as f:
            self.assertEqual(f.read(), "hello world")
        with open(path.join(self.client_root, 'path_two'), 'rb') as f:
            self.assertEqual(f.read(), "hi there")

    def test_upload_changes(self):
        self.server_fixtures(1, {
            'path_one': "hello world",
            'path_two': "hi there",
        })
        self.run_loop()

        data_one = "hello world"
        data_three = "me three"
        with open(path.join(self.client_root, 'path_three'), 'wb') as f:
            f.write(data_three)
        os.unlink(path.join(self.client_root, 'path_one'))
        self.run_loop()

        self.assertEqual(set(os.listdir(self.server_versions_path)),
                         set(['1', '2']))
        with open(path.join(self.server_versions_path, '2'), 'rb') as f:
            version_2_index = f.read()
        new_file_line = ("path_three: {sha1: %s, size: %d}\n" %
                         (sha1hex(data_three), len(data_three)))
        removed_file_line = ("path_one: {sha1: %s, size: %d}\n" %
                             (sha1hex(data_one), len(data_one)))
        self.assertTrue(new_file_line in version_2_index)
        self.assertTrue(removed_file_line not in version_2_index)

    def test_receive_changes(self):
        self.server_fixtures(1, {
            'path_one': "hello world",
            'path_two': "hi there",
        })
        self.run_loop()

        self.server_fixtures(2, {
            'path_one': "hello world",
            'path_three': "me three",
        })
        self.run_loop()

        self.assertEqual(set(os.listdir(self.client_root)),
                         set(['.syncit', 'path_one', 'path_three']))
        with open(path.join(self.client_root, '.syncit/last_sync'), 'rb') as f:
            self.assertEqual(f.read(), "2\n")
        with open(path.join(self.client_root, 'path_one'), 'rb') as f:
            self.assertEqual(f.read(), "hello world")
        with open(path.join(self.client_root, 'path_three'), 'rb') as f:
            self.assertEqual(f.read(), "me three")


if __name__ == '__main__':
    unittest.main()
