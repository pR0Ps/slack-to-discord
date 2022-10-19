#!/usr/bin/env python

from http import HTTPStatus
import http.server
import io
import threading

import pytest

from slack_to_discord import SeekableHTTPStream


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
                self.send_header("Content-Range", f"bytes {start}-{end-1}/{end}")
        else:
            self.send_response(HTTPStatus.OK)

        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Content-Length", RESP_SIZE)
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


def test_server(mockserver):
    s = SeekableHTTPStream(mockserver, chunk_size=10)

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

    # test seeking bck from current pos and getting data we just got just reads
    # it from the buffer
    assert s.seek(-10, io.SEEK_CUR) == 5
    assert s.read(10) == gen_bytes(5, 15)

    # test seeking past the end stops at the end and returns no data
    assert s.seek(RESP_SIZE * 2) == RESP_SIZE
    assert s.read() == b""

    # test read with no args gets everything
    assert s.seek(50) == 50
    assert s.read() == gen_bytes(50, 100)

    # test read1 only reads at most 1 chunk from the stream
    assert s.seek(0) == 0
    assert s.read(5) == gen_bytes(0, 5)
    assert s.read1() == gen_bytes(5, 20) # 5 in the buff + 1 read of 10 bytes

    # test readinto
    b = bytearray(20)
    assert s.seek(5) == 5
    assert s.readinto(b) == 20
    assert s.tell() == 25
    assert bytes(b) == gen_bytes(5, 25)

    # test readinto1 (partial read)
    b = bytearray(20)
    assert s.readinto1(b) == 15
    assert bytes(b) == gen_bytes(25, 40) + bytes(5)
