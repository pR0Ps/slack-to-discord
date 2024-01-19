#!/usr/bin/env python

import functools
from http import HTTPStatus
import http.server
import io
import threading

import pytest

from slack_to_discord.http_stream import SeekableHTTPStream, CachedSeekableHTTPStream


RESP_SIZE = 100


# TODO: make tests for non-seekable and/or unknown length responses

def gen_bytes(s, e):
    return b"".join(bytes([x]) for x in range(s, e))


class HTTPRangeRequestHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        range_ = self.headers.get("Range")
        start, end = 0, RESP_SIZE
        if range_ is not None:
            _, r = range_.split("bytes=", 1)
            s, e = r.split("-", 1)
            if e:
                end = int(e)
            if s:
                start = int(s)

            if not 0 <= start < end <= RESP_SIZE:
                self.send_response(HTTPStatus.REQUESTED_RANGE_NOT_SATISFIABLE)
                self.end_headers()
                return
            else:
                self.send_response(HTTPStatus.PARTIAL_CONTENT)
                self.send_header("Content-Range", f"bytes {start}-{end-1}/{RESP_SIZE}")
        else:
            self.send_response(HTTPStatus.OK)

        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Content-Length", end - start)
        self.end_headers()
        self.wfile.write(gen_bytes(start, end))


@pytest.fixture(scope="module")
def mockserver():
    s = http.server.HTTPServer(("127.0.0.1", 0), HTTPRangeRequestHandler)
    thread = threading.Thread(target=s.serve_forever)
    thread.start()
    try:
        yield f"http://127.0.0.1:{s.server_port}"
    finally:
        s.shutdown()
        thread.join()


@pytest.mark.parametrize("stream_cls", [
    SeekableHTTPStream,
    CachedSeekableHTTPStream,
    functools.partial(CachedSeekableHTTPStream, max_buffer_size=50, force_cache=True)
])
def test_http_stream(mockserver, stream_cls):
    s = stream_cls(mockserver, chunk_size=10)

    assert s.readable()
    assert s.seekable()
    assert not s.writable()

    assert len(s) == RESP_SIZE

    # test reading chunks
    assert s.read(10) == gen_bytes(0, 10)
    assert s.read(10) == gen_bytes(10, 20)
    assert s.read(10) == gen_bytes(20, 30)

    # test seeking
    assert s.seek(10, io.SEEK_CUR) == 40
    assert s.seek(-10, io.SEEK_CUR) == 30
    assert s.seek(0, io.SEEK_END) == RESP_SIZE
    assert s.seek(-10, io.SEEK_END) == RESP_SIZE - 10
    assert s.seek(5, io.SEEK_SET) == 5

    # test reading across chunks
    assert s.read(10) == gen_bytes(5, 15)
    assert s.tell() == 15

    # test seeking back from current pos and getting data we just got just reads
    # it from the buffer
    assert s.seek(-10, io.SEEK_CUR) == 5
    assert s.read(10) == gen_bytes(5, 15)

    # test seeking past the end stops at the end and returns no data
    assert s.seek(RESP_SIZE * 2) == RESP_SIZE
    assert s.read() == b""

    # test read with no args gets everything
    assert s.seek(50) == 50
    assert s.read() == gen_bytes(50, 100)

    # test readinto
    b = bytearray(20)
    assert s.seek(5) == 5
    assert s.readinto(b) == 20
    assert s.tell() == 25
    assert bytes(b) == gen_bytes(5, 25)


def test_cached_http_stream_read_rolls(mockserver):
    s = CachedSeekableHTTPStream(mockserver, chunk_size=10, max_buffer_size=50, force_cache=True)
    assert s._cache

    assert s.read(50) == gen_bytes(0, 50)
    assert not s._cache._rolled

    assert s.read(1) == gen_bytes(50, 51)
    assert s._cache._rolled

    assert s.seek(0) == 0
    assert s.read() == gen_bytes(0, 100)

def test_cached_http_stream_seek_rolls(mockserver):
    s = CachedSeekableHTTPStream(mockserver, chunk_size=10, max_buffer_size=50, force_cache=True)
    assert s._cache

    assert not s._cache._rolled
    assert s.seek(0, io.SEEK_END) == RESP_SIZE
    assert s._cache._rolled
    assert s.read(1) == b""

    assert s.seek(0) == 0
    assert s.read() == gen_bytes(0, 100)
