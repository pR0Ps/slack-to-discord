#!/usr/bin/env python

import io

import urllib3


class IterBuffer:
    """Provides a buffered readable interface for an iterator"""

    def __init__(self, iterator=None):
        self.reset(iterator)

    def __len__(self):
        """How much data is available to be read from the buffer"""
        return len(self._data) - self._pos

    def _buff_read(self, size=-1):
        l = len(self)
        if size < 0 or size > l:
            size = l
        if size < 1:
            return b""

        ret = bytes(self._data if size == l else self._data[self._pos:self._pos+size])
        self._pos += size
        return ret

    def _buff_fill(self, size, read1=False):
        """Ensure enough data is in the buffer"""
        if size == 0 or self._iter is None:
            return

        cur_size = len(self)
        if size > 0 and cur_size >= size:
            # have the data, don't need to read anything
            return

        if self._pos > 0:
            self._data = self._data[self._pos:]
            self._pos = 0

        for x in self._iter:
            self._data += x
            cur_size += len(x)
            if read1 or (size > 0 and cur_size >= size):
                break
        else:
            self._iter = None

    def relseek(self, num):
        """Relative seek within the buffer

        Given the number of bytes to seek within the buffer (-/+), get as close
        as possible and return the actual seek amount
        """
        actual = max(-self._pos, min(num, len(self)))
        self._pos += actual
        return actual

    def read(self, size=-1):
        """Read and return up to `size` bytes.

        If the argument is omitted or negative, data is read and returned until
        EOF is reached. An empty bytes object is returned if the stream is
        already at EOF
        """
        self._buff_fill(size)
        return self._buff_read(size)

    def read1(self, size=-1):
        """Read and return up to `size` bytes with at most one call to the iterator"""
        self._buff_fill(size, read1=True)
        return self._buff_read(size)

    def reset(self, iterator=None):
        """Zero out the current buffer and assign a new iterator to buffer bytes from"""
        self._iter = iterator
        self._data = bytearray()
        self._pos = 0


class SeekableHTTPStream(io.BufferedIOBase):
    """Make the contents at a URL addressable via a seekable file-like object.

    Seeking to arbitrary offsets is handled using HTTP range requests.

    Notes:
     - The server must respond with an `Accept-Ranges: bytes` header for seeking to be supported.
     - Using `__len__` or `io.SEEK_END` to seek requires the server to have sent a valid `Content-Length` header.
    """

    def __init__(self, url, chunk_size=64*1024):
        self._pos = 0
        self._url = url
        self._pool = urllib3.PoolManager()
        self._resp = None
        self._buff = IterBuffer()

        self._chunk_size = chunk_size
        self._do_request()

        try:
            self._content_length = int(self._resp.getheader("Content-Length"))
        except TypeError:
            self._content_length = None

        self._seekable = self._resp.getheader("Accept-Ranges", "").lower() == "bytes"

    def _reset(self):
        if self._resp:
            # release the connection back into the pool
            self._resp.release_conn()
        self._resp = None
        self._buff.reset()

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
            self._buff.reset(resp.stream(amt=self._chunk_size, decode_content=True))

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
        ret = self._buff.read(size)
        self._pos += len(ret)
        return ret

    def read1(self, size=-1):
        ret = self._buff.read1(size)
        self._pos += len(ret)
        return ret

    def readinto(self, b):
        data = self.read(len(b))
        l = len(data)
        b[:l] = data
        return l

    def readinto1(self, b):
        data = self.read1(len(b))
        l = len(data)
        b[:l] = data
        return l

    def seek(self, offset, whence=io.SEEK_SET):
        if not self.seekable():
            raise io.UnsupportedOperation(f"URL '{self._url}' does not support range requests")

        if whence == io.SEEK_SET:
            new_pos = offset
        elif whence == io.SEEK_CUR:
            new_pos = self._pos + offset
        elif whence == io.SEEK_END:
            if self._content_length is None:
                raise io.UnsupportedOperation("can't do end-relative seeks without knowing the length")
            new_pos = len(self) + offset
        else:
            raise ValueError("Invalid whence: {}".format(whence))

        if self._content_length is not None:
            new_pos = min(new_pos, self._content_length)
        new_pos = max(0, new_pos)

        if new_pos == self._content_length:
            # seeking to end - no more data
            self._reset()
        else:
            # Figure out how far off we are and seek within the buffered data
            # to get as close as possible to the target offset
            pos_diff = new_pos - self._pos
            pos_diff -= self._buff.relseek(pos_diff)

            if pos_diff == 0:
                # seeked to an already-buffered offset - nothing else to do
                pass
            elif 0 < pos_diff < self._chunk_size * 2:
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
