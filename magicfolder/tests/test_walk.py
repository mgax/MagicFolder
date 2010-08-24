import unittest
import py
from magicfolder.checksum import repo_file_events

class WalkTest(unittest.TestCase):
    def setUp(self):
        self.tmp = py.path.local.mkdtemp()
        self.tmp.mkdir('.mf')
        a = self.tmp.mkdir('fa')
        a.join('image.jpg').write("file one")
        a.join('image.png').write("file two")
        b = self.tmp.mkdir('fb')
        b.join('photo.png').write("file three")

    def tearDown(self):
        self.tmp.remove()

    def walk_repo(self):
        return set(i.path for i in repo_file_events(str(self.tmp)))

    def test_excludes(self):
        # no exclude
        assert ( self.walk_repo() ==
                 set(['fa/image.jpg', 'fa/image.png', 'fb/photo.png']) )

        # exclude specific file
        self.tmp.join('.mfignore').write('image.png\n')
        assert ( self.walk_repo() ==
                 set(['fa/image.jpg', 'fb/photo.png', '.mfignore']) )

        # exclude specific folder
        self.tmp.join('.mfignore').write('fa\n')
        assert self.walk_repo() == set(['fb/photo.png', '.mfignore'])

if __name__ == '__main__':
    unittest.main()
