#!/usr/bin/env python

import io

import urllib3


class IteratorReader(io.RawIOBase):
    """Provides an io.RawIOBase-compatible interface for an iterator"""

    def __init__(self, iterator):
        self._iter = iter(iterator)
        self._extra = bytearray()
        self._total = 0

    def readable(self):
        return True

    def tell(self):
        """Return the total number of bytes that have been read so far"""
        if self.closed:
            raise ValueError("I/O operation on a closed {}".format(self.__class__.__name__))
        return self._total - len(self._extra)

    def readinto(self, b):
        """Read bytes into a pre-allocated bytes-like object b

        Returns the number of bytes read, 0 indicates EOF
        """
        num = len(b)
        if self._iter is not None:
            while len(self._extra) < num:
                try:
                    new = next(self._iter)
                except StopIteration:
                    self._iter = None
                    break
                else:
                    self._total += len(new)
                    self._extra += new

        ret, self._extra = self._extra[:num], self._extra[num:]

        lret = len(ret)
        b[:lret] = ret
        return lret


class BufferedIteratorReader(io.BufferedReader):
    """Provides a io.BufferedReader interface over an IteratorReader"""

    def __init__(self, iterator, buffer_size=io.DEFAULT_BUFFER_SIZE):
        """Create a new buffered iterator reader using the given iterator"""
        super().__init__(raw=IteratorReader(iterator), buffer_size=buffer_size)


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
            self._buff = BufferedIteratorReader(
                resp.stream(amt=self._chunk_size, decode_content=True),
                buffer_size=self._chunk_size
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
