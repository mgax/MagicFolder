import unittest
import tempfile
import shutil
import os
from os import path
from hashlib import sha1

from magicfolder.blobdb import BlobDB

data = {
    'f1': 'file one',
    'f2': 'file two',
    'f3': 'file three',
}
sha = dict( (name, sha1(data[name]).hexdigest()) for name in data )

def backup_the_files(db, names=data.keys()):
    for name in names:
        with db.write_file() as f:
            f.write(data[name])

class BackupTest(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

    def test_backup(self):
        test_backup = BlobDB(self.tmpdir)
        backup_the_files(test_backup)

        self.assertEqual(set(os.listdir(self.tmpdir)),
                         set([sha['f1'][:2], sha['f2'][:2], sha['f3'][:2]]))

        self.assertEqual(os.listdir(path.join(self.tmpdir, sha['f1'][:2])),
                         [sha['f1'][2:]])
        with open(path.join(self.tmpdir, sha['f1'][:2], sha['f1'][2:]),
                  'rb') as f:
            self.assertEqual(f.read(), 'file one')

        self.assertEqual(os.listdir(path.join(self.tmpdir, sha['f2'][:2])),
                         [sha['f2'][2:]])
        with open(path.join(self.tmpdir, sha['f2'][:2], sha['f2'][2:]),
                  'rb') as f:
            self.assertEqual(f.read(), 'file two')

        self.assertEqual(os.listdir(path.join(self.tmpdir, sha['f3'][:2])),
                         [sha['f3'][2:]])
        with open(path.join(self.tmpdir, sha['f3'][:2], sha['f3'][2:]),
                  'rb') as f:
            self.assertEqual(f.read(), 'file three')

    def test_backup_contains(self):
        test_backup = BlobDB(self.tmpdir)
        backup_the_files(test_backup)

        self.assertTrue('62a837970950bf34fb0c'
                        '401c39cd3c0d373f0a7a' in test_backup)
        self.assertTrue('62a837970950bf34fb0c'
                        '00000000000000000000' not in test_backup)
