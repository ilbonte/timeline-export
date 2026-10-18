import base64
import contextlib
import io
from pathlib import Path
import sqlite3
import struct
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

import geller_fetch as g


def message(segment, end, continuation=None):
    columns = ('segment_id', 'semantic_segment', 'end_timestamp_seconds')
    values = g.fld_bytes(1, g.fld_str(3, segment))
    values += g.fld_bytes(1, g.fld_bytes(4, b'synthetic-segment'))
    values += g.fld_bytes(1, g.fld_varint(1, end))
    snap = g.fld_str(1, 'semantic_segment_table')
    snap += b''.join(g.fld_str(4, col) for col in columns) + g.fld_bytes(5, values)
    payload = g.fld_str(1, 'type.googleapis.com/geller.externaldb.ExternalDbSync')
    payload += g.fld_bytes(2, g.fld_bytes(3, snap))
    result = g.fld_varint(1, g.ODLH) + g.fld_bytes(5, g.fld_bytes(3, payload))
    if continuation: result += g.fld_bytes(4, continuation)
    # The outer field 4 is not a download pagination flag.
    return g.fld_bytes(1, g.fld_bytes(2, result) + g.fld_varint(3, g.ODLH) + g.fld_bytes(4, b''))


def response(*messages, status=200, grpc_status='0'):
    return SimpleNamespace(status_code=status, headers={'grpc-status': grpc_status},
                           content=b''.join(g.grpc_frame(m) for m in messages))


class GellerFetchTest(unittest.TestCase):
    def run_fetch(self, responses, existing=None):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)
            key = path / 'key.b64'
            key.write_text(base64.b64encode(b'x' * 32).decode())
            db, raw = path / 'backup.db', path / 'raw.bin'
            if existing is not None: db.write_bytes(existing)
            requests = []
            pending = iter(responses)

            def post(url, **kwargs):
                requests.append(g.parse(g.grpc_unframe(kwargs['content'])[0]))
                return next(pending)

            client = mock.MagicMock()
            client.__enter__.return_value = client
            client.post.side_effect = post
            httpx = SimpleNamespace(Client=mock.Mock(return_value=client))
            error = None
            argv = ['geller_fetch.py', '--token', 'synthetic-token', '--key-file', str(key),
                    '--sync-token', 'synthetic-sync-token', '-o', str(db), '--raw', str(raw)]
            with mock.patch.dict(sys.modules, {'httpx': httpx}), mock.patch.object(sys, 'argv', argv):
                with contextlib.redirect_stderr(io.StringIO()), contextlib.redirect_stdout(io.StringIO()):
                    try:
                        g.main()
                    except SystemExit as e:
                        error = str(e)
            rows = None
            if error is None:
                with contextlib.closing(sqlite3.connect(db)) as con:
                    rows = con.execute('SELECT segment_id, end_timestamp_seconds FROM semantic_segment_table ORDER BY segment_id').fetchall()
            return error, requests, rows, db.read_bytes() if db.exists() else None, raw.read_bytes() if raw.exists() else None

    def test_follows_continuation_and_keeps_every_page(self):
        pages = [response(message('earlier', 1700000000, b'synthetic-continuation')),
                 response(message('recent', 1800000000))]
        error, requests, rows, _, raw = self.run_fetch(pages)
        self.assertIsNone(error)
        self.assertEqual(rows, [('earlier', 1700000000), ('recent', 1800000000)])
        self.assertEqual(raw, b''.join(p.content for p in pages))
        self.assertEqual(len(requests), 2)
        for i, request in enumerate(requests):
            item = g.parse(g.one(request, 1))
            self.assertEqual(g.one(request, 2), b'SEMANTICLOCATION')
            self.assertEqual(g.one(item, 1), g.ODLH)
            self.assertEqual(g.one(item, 2), None if i else b'synthetic-sync-token')
            self.assertEqual(g.one(item, 3), b'synthetic-continuation' if i else None)
            self.assertNotIn(4, item)
            self.assertNotIn(5, item)

    def test_reads_all_frames(self):
        error, requests, rows, _, _ = self.run_fetch([
            response(message('first', 1700000000), message('second', 1800000000))])
        self.assertIsNone(error)
        self.assertEqual(len(requests), 1)
        self.assertEqual(len(rows), 2)

    def test_incomplete_download_preserves_previous_database(self):
        first = response(message('earlier', 1700000000, b'synthetic-continuation'))
        item_error = g.fld_bytes(1, g.fld_bytes(1, g.fld_varint(1, 13)))
        truncated_payload = response(message('recent', 1800000000))
        body = truncated_payload.content
        truncated_payload.content = body[:1] + struct.pack('>I', len(body) - 4) + body[5:]
        truncated_header = response(message('recent', 1800000000))
        truncated_header.content += b'\x00'
        failures = [response(status=503), response(grpc_status='13'), response(), response(item_error),
                    first, truncated_payload, truncated_header]
        for failure in failures:
            with self.subTest(status=failure.status_code, body_length=len(failure.content)):
                error, requests, _, db, raw = self.run_fetch([first, failure], existing=b'previous-good-db')
                self.assertIsNotNone(error)
                self.assertEqual(len(requests), 2)
                self.assertEqual(db, b'previous-good-db')
                self.assertIsNone(raw)
                self.assertNotIn('synthetic-continuation', error)


if __name__ == '__main__':
    unittest.main()
