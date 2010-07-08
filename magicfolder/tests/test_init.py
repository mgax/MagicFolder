import unittest
import tempfile
import shutil
import os

from magicfolder.server import server_init

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

if __name__ == '__main__':
    unittest.main()
