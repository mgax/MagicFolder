import unittest
import tempfile
import shutil
import os
from os import path
from hashlib import sha1
from collections import deque

from magicfolder.picklemsg import Remote
from magicfolder.blobdb import BlobDB
from magicfolder.server import server_sync
from magicfolder.checksum import FileItem, read_version_file

from test_sync import TestClientRepo

def sha1hex(s):
    return sha1(s).hexdigest()

def quick_file_item(file_path, file_data):
    return FileItem(file_path, sha1hex(file_data), len(file_data), None)

f1_data = 'some data'
f1 = quick_file_item('file_one', f1_data)

f2_data = 'some more data'
f2 = quick_file_item('file_two', f2_data)

f2a_data = 'some different data'
f2a = quick_file_item('file_two', f2a_data)

f2big_data = '0123456789abcdef' * 64 * 1200 # 1.2 MB of data
f2big = quick_file_item('file_two', f2big_data)

f3_data = 'third data'
f3 = quick_file_item('file_three', f3_data)

f3a_data = 'third data, changed'
f3a = quick_file_item('file_three', f3a_data)

f4_data = 'fourth data'
f4 = quick_file_item('file_four', f4_data)

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
                "Bad message from remote: %r, expected %r."
                % (received, expected))

    def recv(self):
        try:
            msg, payload = next(self.chatter_script)
        except StopIteration:
            assert False, ("Chatter script done, but remote expected more."
                           "Queue is now: %r" % list(self.queue))
        else:
            return msg, payload

    def done(self):
        unread_chatter = list(self.chatter_script)
        assert len(unread_chatter) == 0, "Unread chatter: %r" % unread_chatter
        assert len(self.queue) == 0, "Queue not empty: %r" % list(self.queue)

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
            client.expect('sync', 0)

            yield 'waiting_for_files', None
            client.expect('done', None)

            yield 'sync_complete', 0
            client.expect('quit', None)
            yield 'bye', None

        self.init_client(0, {})
        self.chat_client(test_chat)

    def test_enumerate_files(self):
        def test_chat(client):
            client.expect('sync', 0)

            yield 'waiting_for_files', None
            client.expect('file_meta', f1)
            client.expect('file_meta', f2)
            client.expect('done', None)

            yield 'sync_complete', 1
            client.expect('quit', None)
            yield 'bye', None

        self.init_client(0, {
            'file_one': 'some data',
            'file_two': 'some more data',
        })
        self.chat_client(test_chat)

    def test_upload_files(self):
        def test_chat(client):
            client.expect('sync', 0)

            yield 'waiting_for_files', None
            client.expect('file_meta', f1)
            client.expect('file_meta', f2big)
            client.expect('done', None)

            yield 'data', 'baf34551fecb48acc3da868eb85e1b6dac9de356'
            client.expect('file_chunk', 'some data')
            client.expect('file_end', None)

            yield 'data', '311d6913794296d8bc3557fa8745d938bf9c7b87'
            for c in range(18):
                client.expect('file_chunk', '0123456789abcdef' * 4096)
            client.expect('file_chunk', '0123456789abcdef' * 3072)
            client.expect('file_end', None)

            yield 'sync_complete', 1
            client.expect('quit', None)

            yield 'bye', None

        self.init_client(0, {
            'file_one': 'some data',
            'file_two': '0123456789abcdef' * 64 * 1200 # 1.2 MB of data
        })
        self.chat_client(test_chat)

    def test_download_files(self):
        def test_chat(client):
            client.expect('sync', 0)

            yield 'waiting_for_files', None
            client.expect('done', None)

            yield 'file_begin', f1
            yield 'file_chunk', 'some data'
            yield 'file_end', None

            yield 'file_begin', f2big
            for c in range(18):
                yield 'file_chunk', '0123456789abcdef' * 4096
            yield 'file_chunk', '0123456789abcdef' * 3072
            yield 'file_end', None

            yield 'sync_complete', 1
            client.expect('quit', None)
            yield 'bye', None

        self.init_client(0, {})
        self.chat_client(test_chat)
        self.assertEqual(set(os.listdir(self.tmp_path)),
                         set(['.mf', 'file_one', 'file_two']))

        with open(path.join(self.tmp_path, 'file_one'), 'rb') as f:
            file_one_data = f.read()
        self.assertEqual(file_one_data, 'some data')

        with open(path.join(self.tmp_path, 'file_two'), 'rb') as f:
            for c in range(12):
                self.assertEqual(f.read(102400), '0123456789abcdef' * 6400)
            self.assertEqual(f.read(), '')

    def test_remove_files(self):
        def test_chat(client):
            client.expect('sync', 0)
            yield 'waiting_for_files', None

            client.expect('file_meta', f1)
            client.expect('file_meta', f2)

            client.expect('done', None)
            yield 'file_remove', f1
            yield 'sync_complete', 1

            client.expect('quit', None)
            yield 'bye', None

        self.init_client(0, {
            'file_one': 'some data',
            'file_two': 'some more data'
        })
        self.chat_client(test_chat)
        self.assertEqual(set(os.listdir(self.tmp_path)),
                         set(['.mf', 'file_two']))

class ServerChatterTest(unittest.TestCase):
    def setUp(self):
        self.tmp_path = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp_path)

    def init_server(self, version_trees):
        os.mkdir(self.tmp_path + '/versions')
        os.mkdir(self.tmp_path + '/objects')
        objects = BlobDB(self.tmp_path + '/objects')

        for v, file_tree in version_trees.iteritems():
            with open(self.tmp_path + '/versions/%d' % v, 'wb') as ver_f:
                for i, data in file_tree.iteritems():
                    ver_f.write('"%s" %10d "%s"\n' %
                                (i.checksum, i.size, i.path))
                    with objects.write_file(i.checksum) as data_f:
                        data_f.write(data)

    def chat_server(self, test_chat):
        mock_remote = MockRemote(test_chat)
        server_sync(self.tmp_path, mock_remote)
        mock_remote.done()

    def test_blank(self):
        def test_chat(server):
            yield 'sync', 0
            server.expect('waiting_for_files', None)

            yield 'done', None
            server.expect('sync_complete', 0)

            yield 'quit', None
            server.expect('bye', None)

        self.init_server({0: {}})
        self.chat_server(test_chat)

    def test_upload_files(self):
        def test_chat(server):
            yield 'sync', 1

            server.expect('waiting_for_files', None)
            yield 'file_meta', f1
            yield 'file_meta', f2
            yield 'done', None

            server.expect('data', f2.checksum)
            yield 'file_chunk', f2_data
            yield 'file_end', None

            server.expect('sync_complete', 2)
            yield 'quit', None

            server.expect('bye', None)

        self.init_server({
            0: {},
            1: {f1: f1_data},
        })
        self.chat_server(test_chat)
        self.assertEqual(set(os.listdir(self.tmp_path + '/objects')),
                         set(['83', 'ba']))

    def test_download_files(self):
        def test_chat(server):
            yield 'sync', 1

            server.expect('waiting_for_files', None)
            yield 'file_meta', f1
            yield 'done', None

            server.expect('file_begin', f2)
            server.expect('file_chunk', f2_data)
            server.expect('file_end', None)

            server.expect('sync_complete', 2)
            yield 'quit', None
            server.expect('bye', None)

        self.init_server({
            0: {},
            1: {f1: f1_data},
            2: {f1: f1_data, f2: f2_data},
        })
        self.chat_server(test_chat)

    def test_remove_files(self):
        def test_chat(server):
            yield 'sync', 1

            server.expect('waiting_for_files', None)
            yield 'file_meta', f1
            yield 'file_meta', f2
            yield 'done', None

            server.expect('file_remove', f2)

            server.expect('sync_complete', 2)
            yield 'quit', None
            server.expect('bye', None)

        self.init_server({
            0: {},
            1: {f1: f1_data, f2: f2_data},
            2: {f1: f1_data},
        })
        self.chat_server(test_chat)

    def test_merge_simple(self):
        def test_chat(server):
            yield 'sync', 1

            server.expect('waiting_for_files', None)
            yield 'file_meta', f2
            yield 'file_meta', f4
            yield 'done', None

            server.expect('data', f4.checksum)
            yield 'file_chunk', f4_data
            yield 'file_end', None

            server.expect('file_remove', f2)

            server.expect('file_begin', f3)
            server.expect('file_chunk', f3_data)
            server.expect('file_end', None)

            server.expect('sync_complete', 3)
            yield 'quit', None
            server.expect('bye', None)

        self.init_server({
            0: {},
            1: {f1: f1_data, f2: f2_data},
            2: {f1: f1_data, f3: f3_data},
        })
        self.chat_server(test_chat)
        with open(self.tmp_path + '/versions/3') as vf:
            v3 = set(read_version_file(vf))
        self.assertEqual(v3, set([f3, f4]))

    def test_merge_removed_but_changed(self):
        def test_chat(server):
            yield 'sync', 1

            server.expect('waiting_for_files', None)
            yield 'file_meta', f2a
            yield 'done', None

            server.expect('data', f2a.checksum)
            yield 'file_chunk', f2a_data
            yield 'file_end', None

            server.expect('file_begin', f3a)
            server.expect('file_chunk', f3a_data)
            server.expect('file_end', None)

            server.expect('sync_complete', 3)
            yield 'quit', None
            server.expect('bye', None)

        self.init_server({
            0: {},
            1: {f2: f2_data, f3: f3_data},
            2: {f3a: f3a_data},
        })
        self.chat_server(test_chat)
        with open(self.tmp_path + '/versions/3') as vf:
            v3 = set(read_version_file(vf))
        self.assertEqual(v3, set([f2a, f3a]))


if __name__ == '__main__':
    unittest.main()
