#!/usr/bin/env python

from itertools import tee

import pytest

from slack_to_discord.http_stream import BufferedIteratorReader


def gen_chunks(n, cs=1):
    i = [iter(bytes([x % 256]) for x in range(1, n+1))] * cs
    yield from (b"".join(b) for b in zip(*i))


def test_iteratorreader():

    g1, g2 = tee(gen_chunks(100, 5))

    i = BufferedIteratorReader(g1)
    r = b"".join(g2)

    assert i.readable()
    assert not i.seekable()
    assert not i.writable()

    cnt = 0
    for amt in (0, 1, 2, 3, 4, 5, 10, 1, 1, 0):
        d = i.read(amt)
        assert len(d) == amt
        assert d == r[cnt:cnt+amt]
        cnt += amt
        assert i.tell() == cnt

    assert i.read() == r[cnt:]
    assert i.read() == b""

    assert i.tell() == len(r)


def test_close():

    i = BufferedIteratorReader(gen_chunks(10))

    assert i.read(0) == b""
    assert i.read(1) == b"\x01"
    assert not i.closed

    i.close()
    assert i.closed

    with pytest.raises(ValueError, match="closed"):
        i.read()
    with pytest.raises(ValueError, match="closed"):
        i.read1()
    with pytest.raises(ValueError, match="closed"):
        i.readinto(bytearray())
    with pytest.raises(ValueError, match="closed"):
        i.readinto1(bytearray())
    with pytest.raises(ValueError, match="closed"):
        i.tell()

def test_yield_empty_bytes():
    """Test that an iterator is only 'done' when it stops yielding, not when it yields empty bytes"""
    def gen():
        yield b"1"
        yield b""
        yield b"2"
        yield b""
        yield b""
        yield b"3"
        yield b""
        yield b"4"

    assert BufferedIteratorReader(gen()).read() == b"1234"


def test_readinto():

    i = BufferedIteratorReader(gen_chunks(100, 5))

    b = bytearray(1)
    assert b == b"\00"

    assert i.readinto(b) == 1
    assert len(bytes(b).rstrip(b"\x00")) == 1

    # unlocked read fills the buffer
    b = bytearray(50)
    assert i.readinto(b) == 50
    assert len(bytes(b).rstrip(b"\x00")) == 50

    i.readinto(bytearray(100))
    assert i.readinto(bytearray(100)) == 0  # EOF


def test_readline():
    def gen():
        yield b"this is a line\n"
        yield b"another line\n"
        yield b"another line1\n"
        yield b"another line2\n"
        yield b"another line_"
        yield b""
        yield b""
        yield b"_a"
        yield b"a"
        yield b"aaaaaaa\nbbbbbbbb"
        yield b"_"
        yield b"1"
        yield b"2"
        yield b"3"
        yield b"4"
        yield b"5"
        yield b"_line line line another line actually\n"
        yield b"another line\n"
        yield b"ending line\n"
        yield b"actual ending line no trailing newline"

    lines = BufferedIteratorReader(gen()).readlines()
    lines2 = list(BufferedIteratorReader(gen()))
    real = b"".join(gen()).split(b"\n")

    assert lines == lines2
    assert len(lines) == len(real)
    assert b"".join(lines) == b"\n".join(real)
