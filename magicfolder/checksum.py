import os
from os import path
from collections import namedtuple
from hashlib import sha1
import cPickle as pickle
from itertools import imap
from contextlib import contextmanager
import re
import json

FileItem = namedtuple('FileItem', 'path checksum size time')

CHUNK_SIZE = 64 * 1024 # 64 KB

def repo_files(root_path):
    assert not root_path.endswith('/')
    for parent_path, dir_names, file_names in os.walk(root_path):
        parent_rel_path = parent_path[len(root_path):]
        if parent_rel_path == '':
            dir_names.remove('.mf')
        for name in file_names:
            yield (parent_rel_path + '/' + name)[1:]

def repo_file_events(root_path, use_cache=False):
    cache_path = path.join(root_path, '.mf/cache')

    if use_cache and path.isfile(cache_path):
        with open(cache_path, 'rb') as f:
            cache = pickle.load(f)
    else:
        cache = {}

    new_cache = {}

    for file_path in repo_files(root_path):
        file_full_path = path.join(root_path, file_path)
        file_stat = os.stat(file_full_path)
        file_size = file_stat.st_size
        file_time = file_stat.st_mtime

        file_item = None

        if file_path in cache:
            cached_item = cache[file_path]
            if (file_size, file_time) == (cached_item.size, cached_item.time):
                file_item = cached_item

        if file_item is None:
            sha1_hash = sha1()
            size_count = 0
            with open(file_full_path, 'rb') as f:
                while True:
                    data = f.read(CHUNK_SIZE)
                    if not data:
                        break
                    sha1_hash.update(data)
                    size_count += len(data)
            assert size_count == file_size
            file_checksum = sha1_hash.hexdigest()

            file_item = FileItem(file_path, file_checksum,
                                 file_size, file_time)

        yield file_item
        new_cache[file_path] = file_item

    with open(cache_path, 'wb') as f:
        pickle.dump(new_cache, f, protocol=2)

file_item_pattern = re.compile(r'^(?P<checksum>"[0-9a-f]{40}")\s*'
                               r'(?P<size>\d+)\s*'
                               r'(?P<path>".*")\s*$')

def string_to_file_item(s):
    m = file_item_pattern.match(s)
    assert m is not None, "malformed file entry: %r" % s
    return FileItem(json.loads(m.group('path')),
                    json.loads(m.group('checksum')),
                    int(m.group('size')),
                    None)

def file_item_to_string(file_item):
    return "%s %10d %s" % (json.dumps(file_item.checksum),
                           file_item.size,
                           json.dumps(file_item.path))

def read_version_file(fh):
    return imap(string_to_file_item, fh)

@contextmanager
def write_version_file(fh):
    def write_file_item(file_item):
        fh.write(file_item_to_string(file_item) + '\n')

    yield write_file_item
