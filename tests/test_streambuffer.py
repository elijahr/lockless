"""Tests for StreamBuffer zero-copy bipartite memoryview operations (CRIT-01)."""

from __future__ import annotations

import pytest
from lockless import Closed, Full, StreamBuffer


def test_streambuffer_contiguous_zero_copy() -> None:
    sb = StreamBuffer(64)
    assert sb.capacity == 64
    assert sb.available_read == 0
    assert sb.available_write == 64
    assert sb.is_empty is True
    assert sb.is_full is False

    payload = b"hello zero-copy world!"
    with sb.reserve_write(len(payload)) as (v1, v2):
        assert len(v1) == len(payload)
        assert len(v2) == 0
        v1[:] = payload

    assert sb.available_read == len(payload)
    assert sb.available_write == 64 - len(payload)

    with sb.peek_read() as (r1, r2):
        assert r1.readonly is True
        assert r2.readonly is True
        assert bytes(r1) + bytes(r2) == payload
    sb.consume_read(len(payload))
    assert sb.is_empty is True


def test_streambuffer_bipartite_wraparound() -> None:
    sb = StreamBuffer(16, virtual_mirror=False)
    # Advance write and read cursors to offset 12 of 16
    assert sb.write(b"0123456789ab") == 12
    assert sb.read(12) == b"0123456789ab"
    assert sb.available_read == 0
    assert sb.available_write == 16

    # Reserve 10 bytes across the 16-byte wrap boundary:
    # first segment should be 4 bytes (indices 12..15), second segment 6 bytes (indices 0..5)
    data = b"ABCDEFGHIJ"
    with sb.reserve_write(10) as (w1, w2):
        assert len(w1) == 4
        assert len(w2) == 6
        w1[:] = data[:4]
        w2[:] = data[4:]

    assert sb.available_read == 10
    assert sb.available_write == 6

    # Peek all 10 bytes: should also return (4 bytes, 6 bytes) across wrap boundary
    with sb.peek_read() as (r1, r2):
        assert len(r1) == 4
        assert len(r2) == 6
        assert bytes(r1) == b"ABCD"
        assert bytes(r2) == b"EFGHIJ"

    # Partial peek with max_bytes=7
    with sb.peek_read(7) as (p1, p2):
        assert len(p1) == 4
        assert len(p2) == 3
        assert bytes(p1) + bytes(p2) == b"ABCDEFG"

    sb.consume_read(10)
    assert sb.is_empty is True


def test_streambuffer_direct_tuple_unpack_and_readinto() -> None:
    sb = StreamBuffer(32)
    w1, w2 = sb.reserve_write(8)
    assert len(w2) == 0
    w1[:] = b"12345678"
    sb.commit_write(8)

    target = bytearray(8)
    n = sb.readinto(target)
    assert n == 8
    assert bytes(target) == b"12345678"
    assert sb.is_empty is True


def test_streambuffer_exception_rolls_back_write_reservation() -> None:
    sb = StreamBuffer(32)
    with pytest.raises(RuntimeError, match="abort write"), sb.reserve_write(16) as (w1, _w2):
        w1[:4] = b"test"
        raise RuntimeError("abort write")

    # Because an exception occurred inside the with-block, 0 bytes were committed
    assert sb.available_read == 0
    assert sb.available_write == 32


def test_streambuffer_negative_controls() -> None:
    for bad_cap in (0, 1, 3, 15, -8):
        with pytest.raises(ValueError):
            StreamBuffer(bad_cap)
    with pytest.raises(TypeError):
        StreamBuffer("64")  # type: ignore[arg-type]

    sb = StreamBuffer(16)
    with pytest.raises(Full):
        sb.reserve_write(17)

    # Active reservation blocks close() and destroy() with BufferError (CRIT-01)
    res = sb.reserve_write(8)
    with pytest.raises(BufferError):
        sb.close()
    with pytest.raises(BufferError):
        sb.destroy()
    res.commit(8)

    # Readinto requires writable buffer
    with pytest.raises(TypeError):
        sb.readinto(b"readonly")  # type: ignore[arg-type]

    sb.close()
    assert sb.is_closed is True
    with pytest.raises(Closed):
        sb.write(b"abc")
