import os
import unittest
import gzip
import struct

import pb
MAGIC = 0xFFFFFFFF
DEVICE_APPS_TYPE = 1
TEST_FILE = "test.pb.gz"


class TestPB(unittest.TestCase):
    deviceapps = [{"device": {"type": "idfa", "id": "e7e1a50c0ec2747ca56cd9e1558c0d7c"},
                   "lat": 67.7835424444, "lon": -22.8044005471, "apps": [1, 2, 3, 4]},
                  {"device": {"type": "gaid", "id": "e7e1a50c0ec2747ca56cd9e1558c0d7d"}, "lat": 42, "lon": -42, "apps": [1, 2]},
                  {"device": {"type": "gaid", "id": "e7e1a50c0ec2747ca56cd9e1558c0d7d"}, "lat": 42, "lon": -42, "apps": []},
                  {"device": {"type": "gaid", "id": "e7e1a50c0ec2747ca56cd9e1558c0d7d"}, "apps": [1]},
                  ]

    def tearDown(self):
        os.remove(TEST_FILE)

    def test_write(self):
        bytes_written = pb.deviceapps_xwrite_pb(self.deviceapps, TEST_FILE)
        self.assertTrue(bytes_written > 0)
        with gzip.open(TEST_FILE, 'rb') as f:
            content = f.read()
        self.assertEqual(len(content), bytes_written)
        offset = 0
        for i, _ in enumerate(self.deviceapps):
            r_magic, = struct.unpack('I', content[offset:(offset + 4)])
            self.assertEqual(r_magic, MAGIC)
            offset = offset + 4
            r_device_apps_type, = struct.unpack('H', content[offset:offset + 2])
            self.assertEqual(r_device_apps_type, DEVICE_APPS_TYPE)
            offset = offset + 2
            r_len, = struct.unpack('H', content[offset:offset + 2])
            offset = offset + r_len + 2
        self.assertEqual(offset, bytes_written)

    def test_read(self):
        pb.deviceapps_xwrite_pb(self.deviceapps, TEST_FILE)
        count = 0
        for i, d in enumerate(pb.deviceapps_xread_pb(TEST_FILE)):
            self.assertEqual(d, self.deviceapps[i])
            count += 1
        self.assertEqual(count, len(self.deviceapps))
