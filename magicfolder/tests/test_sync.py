import unittest
import tempfile
import shutil
import os
from os import path
import threading
from Queue import Queue
from hashlib import sha1
from contextlib import contextmanager
from collections import deque

from probity.backup import Backup

from magicfolder.picklemsg import Remote
from magicfolder.client import ClientRepo
from magicfolder.server import server_sync, try_except_send_remote
from magicfolder.picklemsg import Remote

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

class TestClientRepo(ClientRepo):
    def __init__(self, root_path, remote):
        super(TestClientRepo, self).__init__(root_path)
        self._test_remote = remote

    @contextmanager
    def connect_to_remote(self):
        yield self._test_remote

def do_server_loop(root_path, in_queue, out_queue):
    remote = TestRemote(in_queue, out_queue)
    with try_except_send_remote(remote):
        server_sync(root_path, remote)

def do_client_sync(root_path, in_queue, out_queue):
    remote = TestRemote(in_queue, out_queue)
    TestClientRepo(root_path, remote).sync_with_remote()

def do_client_server(client_root, server_root):
    c2s = Queue()
    s2c = Queue()
    server_thread = threading.Thread(target=do_server_loop,
                                     args=(server_root, c2s, s2c))
    server_thread.start()
    do_client_sync(client_root, s2c, c2s)
    server_thread.join()

class FullSyncTest(unittest.TestCase):
    def setUp(self):
        self.client_tmp_path = tempfile.mkdtemp()
        self.client_root = path.join(self.client_tmp_path, 'repo')
        os.mkdir(self.client_root)
        os.mkdir(self.client_root + '/.mf')
        with open(self.client_root + '/.mf/last_sync', 'wb') as f:
            f.write("0\n")

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
                         set(['.mf', 'path_one', 'path_two']))
        with open(path.join(self.client_root, '.mf/last_sync'), 'rb') as f:
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
                         set(['.mf', 'path_one', 'path_three']))
        with open(path.join(self.client_root, '.mf/last_sync'), 'rb') as f:
            self.assertEqual(f.read(), "2\n")
        with open(path.join(self.client_root, 'path_one'), 'rb') as f:
            self.assertEqual(f.read(), "hello world")
        with open(path.join(self.client_root, 'path_three'), 'rb') as f:
            self.assertEqual(f.read(), "me three")

class MockRemote(Remote):
    def __init__(self, chatter_script):
        self.queue = deque()
        self.chatter_script = chatter_script(self)

    def send(self, msg, payload=None):
        self.queue.append( (msg, payload) )

    def script_recv(self):
        assert len(self.queue) > 0
        msg, payload = self.queue.popleft()
        return msg, payload

    def expect(self, *expected):
        received = self.script_recv()
        assert (received == expected), (
                "Bad message from client: %r, expected %r."
                % (received, expected))

    def recv(self):
        try:
            msg, payload = next(self.chatter_script)
        except StopIteration:
            assert False, ("Chatter script done, but client expected more."
                           "Queue is now: %r" % list(self.queue))
        else:
            return msg, payload

    def done(self):
        assert len(self.queue) == 0
        assert len(list(self.chatter_script)) == 0

class ClientChatterTest(unittest.TestCase):
    def setUp(self):
        self.tmp_path = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp_path)

    def init_client(self, last_sync, repo_files):
        os.mkdir(self.tmp_path + '/.mf')
        with open(self.tmp_path + '/.mf/last_sync', 'wb') as f:
            f.write("%d\n" % last_sync)
        for file_path, file_data in repo_files.iteritems():
            file_full_path = path.join(self.tmp_path, file_path)
            folder_path = path.dirname(file_full_path)
            if not path.isdir(folder_path):
                os.makedirs(folder_path)
            with open(file_full_path, 'wb') as f:
                f.write(file_data)

    def chat_client(self, test_chat):
        mock_remote = MockRemote(test_chat)
        TestClientRepo(self.tmp_path, mock_remote).sync_with_remote()
        mock_remote.done()

    def test_empty_sync(self):
        def test_chat(client):
            client.expect('merge', 0)
            yield 'waiting_for_files', None

            client.expect('done', None)
            yield 'sync_complete', 0

            client.expect('quit', None)
            yield 'bye', None

        self.init_client(0, {})
        self.chat_client(test_chat)

    def test_enumerate_files(self):
        def test_chat(client):
            client.expect('merge', 0)
            yield 'waiting_for_files', None

            client.expect('file_meta', {'path': 'file_one', 'size': 9,
                'checksum': 'baf34551fecb48acc3da868eb85e1b6dac9de356'})
            yield 'continue', None

            client.expect('file_meta', {'path': 'file_two', 'size': 14,
                'checksum': '83ca2344ac9901d5590bb59b7be651869ef5fbd9'})
            yield 'continue', None

            client.expect('done', None)
            yield 'sync_complete', 1

            client.expect('quit', None)
            yield 'bye', None

        self.init_client(0, {
            'file_one': 'some data',
            'file_two': 'some more data',
        })
        self.chat_client(test_chat)


if __name__ == '__main__':
    unittest.main()
