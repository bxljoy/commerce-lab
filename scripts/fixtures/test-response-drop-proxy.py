"""Regression tests for the real client's chunked request framing."""
import io
from pathlib import Path
import runpy
import unittest

proxy = runpy.run_path(str(Path(__file__).with_name("response-drop-proxy.py")))


class RequestFramingTest(unittest.TestCase):
    def test_chunked_body_with_extensions_and_trailers(self):
        stream = io.BytesIO(b'4;ext=yes\r\n{"a"\r\n3\r\n:1}\r\n0\r\nTrailer: value\r\n\r\n')
        self.assertEqual(proxy["read_body"]({"Transfer-Encoding": "chunked"}, stream), b'{"a":1}')
        self.assertEqual(stream.read(), b'')

    def test_content_length_and_empty_get(self):
        self.assertEqual(proxy["read_body"]({"Content-Length": "2"}, io.BytesIO(b'{}')), b'{}')
        self.assertEqual(proxy["read_body"]({}, io.BytesIO()), b'')


unittest.main()
