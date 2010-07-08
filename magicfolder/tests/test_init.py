import unittest
import tempfile
import shutil
import os

from magicfolder.server import server_init
from magicfolder.client import client_init

class InitTest(unittest.TestCase):
    def setUp(self):
        self.tmp_path = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp_path)

    def test_init_server(self):
        server_init(self.tmp_path)
        self.assertEqual(set(os.listdir(self.tmp_path)),
                         set(['objects', 'versions']))
        self.assertEqual(os.listdir(self.tmp_path + '/objects'), [])
        self.assertEqual(os.listdir(self.tmp_path + '/versions'), ['0'])
        with open(self.tmp_path + '/versions/0', 'rb') as f:
            ver_0_data = f.read()
        self.assertEqual(ver_0_data, "")

    def test_init_client(self):
        client_init(self.tmp_path, 'the remote path')
        self.assertEqual(os.listdir(self.tmp_path), ['.mf'])
        self.assertEqual(set(os.listdir(self.tmp_path + '/.mf')),
                         set(['last_sync', 'remote']))

        with open(self.tmp_path + '/.mf/last_sync', 'rb') as f:
            last_sync_data = f.read()
        self.assertEqual(last_sync_data, "0\n")

        with open(self.tmp_path + '/.mf/remote', 'rb') as f:
            remote_data = f.read()
        self.assertEqual(remote_data, "the remote path\n")

if __name__ == '__main__':
    unittest.main()
