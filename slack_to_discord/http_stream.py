#!/usr/bin/env python

import io
from tempfile import SpooledTemporaryFile

from iterableio import open_iterable
import urllib3


DEFAULT_CHUNK_SIZE = 64 * 1024  # 64K
"""Chunk size to use to download files"""

DEFAULT_BUFFER_SIZE = 10 * 1024 * 1024  # 10M
"""How much file data to store in memory before dumping it to disk instead

(only used if the remote server doesn't support range requests)
"""


class SeekableHTTPStream(io.BufferedIOBase):
    """Make the contents at a URL addressable via a seekable file-like object.

    Seeking to arbitrary offsets is handled using HTTP range requests.

    Notes:
     - The server must respond with an `Accept-Ranges: bytes` header for seeking to be supported.
     - Using `__len__` or `io.SEEK_END` to seek requires the server to have sent a valid `Content-Length` header.
    """

    def __init__(self, url, chunk_size=DEFAULT_CHUNK_SIZE):
        self._pos = 0
        self._url = url
        self._pool = urllib3.PoolManager()
        self._resp = None
        self._buff = None

        self._chunk_size = chunk_size
        self._do_request()

        try:
            self._content_length = int(self._resp.headers.get("Content-Length"))
        except TypeError:
            self._content_length = None

        self._seekable = self._resp.headers.get("Accept-Ranges", "").lower() == "bytes"

    def _reset(self):
        if self._resp:
            # release the connection back into the pool
            self._resp.release_conn()
        self._resp = None
        self._buff = None

    def _do_request(self, start=0):
        self._reset()

        headers = {}
        if start > 0:
            headers["Range"] = "bytes={}-".format(start)

        resp = self._pool.request(
            "GET",
            self._url,
            headers=headers,
            preload_content=False,
        )
        if start > 0 and self._content_length is None and resp.status == 416:
            # Hit the end of the file - no more data
            self._reset()
        elif resp.status not in (200, 206):
            self._reset()
            raise urllib3.exceptions.HTTPError(
                "Bad status code: {}".format(resp.status)
            )
        else:
            self._resp = resp
            self._buff = open_iterable(
                resp.stream(amt=self._chunk_size, decode_content=True),
                mode="rb",
                buffering=self._chunk_size
            )

    def __len__(self):
        if self._content_length is None:
            raise TypeError("The length of this {} is unknown".format(self.__class__.__name__))
        return self._content_length

    def close(self):
        self._reset()
        super().close()

    def writable(self):
        return False

    def readable(self):
        return True

    def seekable(self):
        return self._seekable

    def tell(self):
        return self._pos

    def detach(self):
        raise io.UnsupportedOperation()

    def read(self, size=-1):
        if self._buff is None:
            return b""
        data = self._buff.read(size)
        self._pos += len(data)
        return data

    def _calc_new_pos(self, offset, whence):
        if whence == io.SEEK_SET:
            new_pos = offset
        elif whence == io.SEEK_CUR:
            new_pos = self.tell() + offset
        elif whence == io.SEEK_END:
            if self._content_length is None:
                raise io.UnsupportedOperation("can't do end-relative seeks without knowing the length")
            new_pos = len(self) + offset
        else:
            raise ValueError("Invalid whence: {}".format(whence))

        if self._content_length is not None:
            new_pos = min(new_pos, self._content_length)

        return max(0, new_pos)

    def seek(self, offset, whence=io.SEEK_SET):
        if not self.seekable():
            raise io.UnsupportedOperation(f"URL '{self._url}' does not support range requests")

        new_pos = self._calc_new_pos(offset, whence)
        if new_pos == self._content_length:
            # seeking to end - no more data
            self._reset()
        elif new_pos != self._pos:
            # Figure out how far off we are and how to deal with it
            pos_diff = new_pos - self._pos

            if 0 < pos_diff < self._chunk_size * 2:
                # seeking forwards and we're close enough (within 2 iterations of
                # the current request) that it makes sense to avoid doing another
                # fresh HTTP request - read the data until we get to the target
                # offset
                self.read(pos_diff)
            else:
                # seekable stream and we're before the current position or far
                # enough ahead that we don't want to read everything up to it - do
                # a range request starting at the requested offset
                self._do_request(new_pos)

        self._pos = new_pos
        return self._pos


class CachedSeekableHTTPStream(SeekableHTTPStream):
    """An adapter for SeekableHTTPStream that ensures that the stream is always seekable

    It does this by only making a single request to the start of the file and
    caching the contents to memory/a tempfile as it's downloaded.
    Seeking forwards will stream more data, seeking backwards will read
    previously-downloaded data out of the cache.
    """

    def __init__(self, *args, max_buffer_size=DEFAULT_BUFFER_SIZE, force_cache=False, **kwargs):
        super().__init__(*args, **kwargs)
        if not force_cache and super().seekable():
            self._cache = None
        else:
            self._cache = SpooledTemporaryFile(max_size=max_buffer_size)
            self._cache_size = 0

    def close(self):
        if self._cache:
            self._cache.close()
        super().close()

    def seekable(self):
        return True

    def tell(self):
        if self._cache is None:
            return super().tell()
        else:
            return self._cache.tell()

    def read(self, size=-1):
        if self._cache is None:
            return super().read(size)

        buff = bytearray()

        buffered = min(size, self._cache_size - self.tell())
        if buffered:
            # get data from the buffer
            buff += self._cache.read(buffered)
            if buffered > 0:
                size -= buffered
        if size:
            # get more data from the stream
            new = super().read(size)
            buff += new
            self._cache_size += self._cache.write(new)

        return bytes(buff)

    def seek(self, offset, whence=io.SEEK_SET):
        if self._cache is None:
            return super().seek(offset, whence=whence)

        new_pos = self._calc_new_pos(offset, whence)
        pos_diff = new_pos - self.tell()

        if pos_diff > 0:
            self.read(pos_diff)
        else:
            self._cache.seek(new_pos)

        return self.tell()
