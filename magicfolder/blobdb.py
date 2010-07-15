import os
from os import path
import tempfile
from hashlib import sha1
from contextlib import contextmanager

from checksum import CHUNK_SIZE

class ChecksumWrapper(object):
    def __init__(self, orig_file):
        self.orig_file = orig_file
        self.sha1_hash = sha1()

    def write(self, data):
        self.orig_file.write(data)
        self.sha1_hash.update(data)

    def close(self):
        self.final_hash = self.sha1_hash.hexdigest()
        del self.sha1_hash

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()

class BlobDB(object):
    def __init__(self, db_path):
        self.db_path = db_path

    @contextmanager
    def write_file(self, checksum=None):
        fd, temp_path = tempfile.mkstemp(dir=self.db_path)
        with os.fdopen(fd, 'wb') as temp_file:
            with ChecksumWrapper(temp_file) as wrapper:
                yield wrapper
            if checksum is not None:
                assert checksum == wrapper.final_hash
            else:
                checksum = wrapper.final_hash

        bucket_path = path.join(self.db_path, checksum[:2])
        blob_path = path.join(bucket_path, checksum[2:])

        if not path.isdir(bucket_path):
            os.makedirs(bucket_path)

        os.rename(temp_path, blob_path)

    @contextmanager
    def read_file(self, checksum):
        # TODO: write tests for this method
        bucket_path = path.join(self.db_path, checksum[:2])
        blob_path = path.join(bucket_path, checksum[2:])
        assert path.isfile(blob_path)
        f = open(blob_path, 'rb')
        yield f
        f.close()

    def __contains__(self, checksum):
        assert isinstance(checksum, str)
        assert len(checksum) == 40
        hash_path = path.join(self.db_path, checksum[:2], checksum[2:])
        return path.isfile(hash_path)
