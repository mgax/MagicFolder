import unittest
from hashlib import sha1

from magicfolder.server import calculate_merge, FileItem

def make_file_item(file_path, file_data):
    return FileItem(file_path, sha1(file_data).hexdigest(), len(file_data), 0)

f1 = make_file_item('file_1', 'some data')
f2 = make_file_item('file_2', 'more data')
f3 = make_file_item('file_3', 'other data')

f2a = make_file_item('file_2', 'more data, changed')
f2b = make_file_item('file_2', 'more data, modified')

class MergeTest(unittest.TestCase):
    def test_blank(self):
        new_tree, conflict = calculate_merge(set(), set(), set())
        self.assertEqual(new_tree, {})
        self.assertEqual(conflict, set())

    def test_only_client(self):
        new_tree, conflict = calculate_merge(set(), set([f1, f2]), set())
        self.assertEqual(new_tree, {'file_1': f1, 'file_2': f2})
        self.assertEqual(conflict, set())

    def test_only_server(self):
        new_tree, conflict = calculate_merge(set(), set(), set([f1, f2]))
        self.assertEqual(new_tree, {'file_1': f1, 'file_2': f2})
        self.assertEqual(conflict, set())

    def test_no_changes(self):
        new_tree, conflict = calculate_merge(
                    set([f1]), set([f1]), set([f1]))
        self.assertEqual(new_tree, {'file_1': f1})
        self.assertEqual(conflict, set())

    def test_add_client(self):
        new_tree, conflict = calculate_merge(
                    set([f1]), set([f1, f2]), set([f1]))
        self.assertEqual(new_tree, {'file_1': f1, 'file_2': f2})
        self.assertEqual(conflict, set())

    def test_add_server(self):
        new_tree, conflict = calculate_merge(
                    set([f1]), set([f1]), set([f1, f2]))
        self.assertEqual(new_tree, {'file_1': f1, 'file_2': f2})
        self.assertEqual(conflict, set())

    def test_add_both(self):
        new_tree, conflict = calculate_merge(
                    set([f1]), set([f1, f2]), set([f1, f3]))
        self.assertEqual(new_tree, {'file_1': f1, 'file_2': f2, 'file_3': f3})
        self.assertEqual(conflict, set())

    def test_add_conflict(self):
        new_tree, conflict = calculate_merge(
                    set([f1]), set([f1, f2]), set([f1, f2a]))
        self.assertEqual(new_tree, {'file_1': f1, 'file_2': f2})
        self.assertEqual(conflict, set([f2a]))

    def test_add_conflict_but_identical(self):
        new_tree, conflict = calculate_merge(
                    set([f1]), set([f1, f2a]), set([f1, f2a]))
        self.assertEqual(new_tree, {'file_1': f1, 'file_2': f2a})
        self.assertEqual(conflict, set([f2a]))

    def test_remove_client(self):
        new_tree, conflict = calculate_merge(
                    set([f1, f2]), set([f1]), set([f1, f2]))
        self.assertEqual(new_tree, {'file_1': f1})
        self.assertEqual(conflict, set())

    def test_remove_server(self):
        new_tree, conflict = calculate_merge(
                    set([f1, f2]), set([f1, f2]), set([f1]))
        self.assertEqual(new_tree, {'file_1': f1})
        self.assertEqual(conflict, set())

    def test_remove_both(self):
        new_tree, conflict = calculate_merge(
                    set([f1, f2]), set([f1]), set([f1]))
        self.assertEqual(new_tree, {'file_1': f1})
        self.assertEqual(conflict, set())

    def test_remove_client_and_change_server(self):
        new_tree, conflict = calculate_merge(
                    set([f1, f2]), set([f1]), set([f1, f2a]))
        self.assertEqual(new_tree, {'file_1': f1, 'file_2': f2a})
        self.assertEqual(conflict, set())

    def test_remove_server_and_change_client(self):
        new_tree, conflict = calculate_merge(
                    set([f1, f2]), set([f1, f2a]), set([f1]))
        self.assertEqual(new_tree, {'file_1': f1, 'file_2': f2a})
        self.assertEqual(conflict, set())

    def test_change_client(self):
        new_tree, conflict = calculate_merge(
                    set([f1, f2]), set([f1, f2a]), set([f1, f2]))
        self.assertEqual(new_tree, {'file_1': f1, 'file_2': f2a})
        self.assertEqual(conflict, set())

    def test_change_server(self):
        new_tree, conflict = calculate_merge(
                    set([f1, f2]), set([f1, f2]), set([f1, f2a]))
        self.assertEqual(new_tree, {'file_1': f1, 'file_2': f2a})
        self.assertEqual(conflict, set())

    def test_change_both(self):
        new_tree, conflict = calculate_merge(
                    set([f1, f2]), set([f1, f2a]), set([f1, f2b]))
        self.assertEqual(new_tree, {'file_1': f1, 'file_2': f2a})
        self.assertEqual(conflict, set([f2b]))

    def test_change_both_but_identical(self):
        new_tree, conflict = calculate_merge(
                    set([f1, f2]), set([f1, f2a]), set([f1, f2a]))
        self.assertEqual(new_tree, {'file_1': f1, 'file_2': f2a})
        self.assertEqual(conflict, set([f2a]))
