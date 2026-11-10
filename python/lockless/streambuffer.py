"""Wait-free SPSC byte stream ring buffer with zero-copy bipartite memoryviews (CRIT-01)."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from ._cabi import (
    CheckStatusError,
    Closed,
    Full,
    ffi,
    lib,
)

_EMPTY_WRITE_BUF = bytearray(0)
_EMPTY_READ_VIEW = memoryview(b"")


def _is_power_of_two(n: int) -> bool:
    return n > 0 and (n & (n - 1)) == 0


def _writable_view(ptr: Any, length: int) -> memoryview:
    if length == 0 or ptr == ffi.NULL:
        return memoryview(_EMPTY_WRITE_BUF)
    return memoryview(ffi.buffer(ptr, length)).cast("B")


def _readonly_view(ptr: Any, length: int) -> memoryview:
    if length == 0 or ptr == ffi.NULL:
        return memoryview(b"")
    return memoryview(ffi.buffer(ptr, length)).cast("B").toreadonly()


class StreamWriteReservation:
    """Zero-copy bipartite write reservation returning ``(first_view, second_view)``.

    Can be used either as a context manager::

        with sb.reserve_write(n) as (v1, v2):
            v1[:] = chunk1
            v2[:] = chunk2

    or unpacked directly as a 2-tuple::

        v1, v2 = sb.reserve_write(n)
        v1[:len(data)] = data
        sb.commit_write(len(data))
    """

    __slots__ = ("_committed", "_released", "_reserved", "_sb", "_v1", "_v2")

    def __init__(self, sb: StreamBuffer, v1: memoryview, v2: memoryview, reserved: int) -> None:
        self._sb = sb
        self._v1 = v1
        self._v2 = v2
        self._reserved = reserved
        self._committed = False
        self._released = False

    @property
    def first(self) -> memoryview:
        return self._v1

    @property
    def second(self) -> memoryview:
        return self._v2

    @property
    def nbytes(self) -> int:
        return self._reserved

    def release(self) -> None:
        if not self._released:
            self._released = True
            self._v1.release()
            self._v2.release()
            self._sb._unregister_write_reservation(self)

    def commit(self, nbytes: int | None = None) -> int:
        """Releases the zero-copy views and commits ``nbytes`` (default: all reserved bytes)."""
        actual = self._reserved if nbytes is None else nbytes
        if actual < 0 or actual > self._reserved:
            raise ValueError(
                f"Cannot commit {actual} bytes on a reservation of {self._reserved} bytes"
            )
        self.release()
        if not self._committed:
            self._committed = True
            if actual > 0:
                rc = lib.lf_stream_commit_write(self._sb._ptr, actual)
                CheckStatusError.check(rc, "StreamBuffer.commit_write")
        return actual

    def __iter__(self) -> Iterator[memoryview]:
        yield self._v1
        yield self._v2

    def __len__(self) -> int:
        return 2

    def __getitem__(self, index: int) -> memoryview:
        if index == 0:
            return self._v1
        if index == 1:
            return self._v2
        raise IndexError("StreamWriteReservation index out of range (expected 0 or 1)")

    def __enter__(self) -> tuple[memoryview, memoryview]:
        return (self._v1, self._v2)

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if exc_type is not None:
            self.release()
            return
        if not self._committed:
            self.commit(self._reserved)
        else:
            self.release()


class StreamReadReservation:
    """Zero-copy bipartite read peek returning read-only ``(first_view, second_view)``.

    Can be used as a context manager or unpacked directly as a 2-tuple::

        with sb.peek_read() as (v1, v2):
            process(v1, v2)
            sb.consume_read(len(v1) + len(v2))
    """

    __slots__ = ("_consumed", "_peeked", "_released", "_sb", "_v1", "_v2")

    def __init__(self, sb: StreamBuffer, v1: memoryview, v2: memoryview, peeked: int) -> None:
        self._sb = sb
        self._v1 = v1
        self._v2 = v2
        self._peeked = peeked
        self._consumed = False
        self._released = False

    @property
    def first(self) -> memoryview:
        return self._v1

    @property
    def second(self) -> memoryview:
        return self._v2

    @property
    def nbytes(self) -> int:
        return self._peeked

    def release(self) -> None:
        if not self._released:
            self._released = True
            self._v1.release()
            self._v2.release()
            self._sb._unregister_read_reservation(self)

    def consume(self, nbytes: int | None = None) -> int:
        """Releases the zero-copy views and advances the read cursor by ``nbytes``."""
        actual = self._peeked if nbytes is None else nbytes
        if actual < 0 or actual > self._peeked:
            raise ValueError(f"Cannot consume {actual} bytes on a peek of {self._peeked} bytes")
        self.release()
        if not self._consumed:
            self._consumed = True
            if actual > 0:
                rc = lib.lf_stream_consume_read(self._sb._ptr, actual)
                CheckStatusError.check(rc, "StreamBuffer.consume_read")
        return actual

    def __iter__(self) -> Iterator[memoryview]:
        yield self._v1
        yield self._v2

    def __len__(self) -> int:
        return 2

    def __getitem__(self, index: int) -> memoryview:
        if index == 0:
            return self._v1
        if index == 1:
            return self._v2
        raise IndexError("StreamReadReservation index out of range (expected 0 or 1)")

    def __enter__(self) -> tuple[memoryview, memoryview]:
        return (self._v1, self._v2)

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.release()


class StreamBuffer:
    """Wait-free SPSC byte stream ring buffer with zero-copy bipartite ``memoryview`` access.

    Parameters
    ----------
    capacity : int, default 4096
        Capacity in bytes. Must be a power of two ``>= 2``.
    virtual_mirror : bool, default False
        When ``False`` (default per CRIT-01), allocates a standard heap buffer and
        exposes wrapped writes/reads as bipartite ``(first_view, second_view)`` pairs.
        When ``True``, attempts OS virtual memory page mirroring.
    """

    __slots__ = (
        "_active_read_reservations",
        "_active_write_reservations",
        "_capacity",
        "_closed",
        "_destroyed",
        "_ptr",
        "_virtual_mirror",
    )

    def __init__(self, capacity: int = 4096, virtual_mirror: bool = False) -> None:
        if not isinstance(capacity, int) or isinstance(capacity, bool):
            raise TypeError("capacity must be an integer")
        if capacity < 2 or not _is_power_of_two(capacity):
            raise ValueError(f"StreamBuffer capacity must be a power of 2 >= 2, got {capacity}")
        ptr = lib.lf_streambuffer_create(capacity, bool(virtual_mirror))
        if ptr == ffi.NULL:
            raise ValueError(
                f"Failed to create StreamBuffer(capacity={capacity}, virtual_mirror={virtual_mirror})"
            )
        self._ptr = ptr
        self._capacity = int(lib.lf_streambuffer_capacity(ptr))
        self._virtual_mirror = bool(virtual_mirror)
        self._closed = False
        self._destroyed = False
        self._active_write_reservations: list[StreamWriteReservation] = []
        self._active_read_reservations: list[StreamReadReservation] = []

    def _check_alive(self) -> None:
        if self._destroyed or self._ptr == ffi.NULL or self._closed:
            raise Closed("StreamBuffer is closed")

    def _unregister_write_reservation(self, res: StreamWriteReservation) -> None:
        try:
            self._active_write_reservations.remove(res)
        except ValueError:
            pass

    def _unregister_read_reservation(self, res: StreamReadReservation) -> None:
        try:
            self._active_read_reservations.remove(res)
        except ValueError:
            pass

    @property
    def capacity(self) -> int:
        return self._capacity

    @property
    def is_closed(self) -> bool:
        return self._closed or self._destroyed or self._ptr == ffi.NULL

    @property
    def available_read(self) -> int:
        if self._destroyed or self._ptr == ffi.NULL:
            return 0
        return int(lib.lf_streambuffer_available_read(self._ptr))

    @property
    def available_write(self) -> int:
        if self._destroyed or self._ptr == ffi.NULL:
            return 0
        return int(lib.lf_streambuffer_available_write(self._ptr))

    @property
    def is_empty(self) -> bool:
        if self._destroyed or self._ptr == ffi.NULL:
            return True
        return bool(lib.lf_streambuffer_is_empty(self._ptr))

    @property
    def is_full(self) -> bool:
        if self._destroyed or self._ptr == ffi.NULL:
            return False
        return bool(lib.lf_streambuffer_is_full(self._ptr))

    def __len__(self) -> int:
        return self.available_read

    def reserve_write(self, nbytes: int) -> StreamWriteReservation:
        """Reserves ``nbytes`` for zero-copy writing as a bipartite ``(first_view, second_view)``.

        Raises ``Full`` if fewer than ``nbytes`` are currently available for writing,
        ``BufferError`` if a write reservation is already active,
        or ``ValueError`` if ``nbytes < 0``.
        """
        self._check_alive()
        if self._active_write_reservations:
            raise BufferError(
                "Cannot reserve_write while an uncommitted write reservation is already active"
            )
        if not isinstance(nbytes, int) or isinstance(nbytes, bool) or nbytes < 0:
            raise ValueError(f"nbytes must be a non-negative integer, got {nbytes}")
        if nbytes > self._capacity:
            raise Full(
                f"Cannot reserve {nbytes} bytes in StreamBuffer of capacity {self._capacity}"
            )
        if nbytes == 0:
            v1 = _writable_view(ffi.NULL, 0)
            v2 = _writable_view(ffi.NULL, 0)
            res = StreamWriteReservation(self, v1, v2, 0)
            self._active_write_reservations.append(res)
            return res

        out_p1 = ffi.new("uint8_t**")
        out_l1 = ffi.new("size_t*")
        out_p2 = ffi.new("uint8_t**")
        out_l2 = ffi.new("size_t*")
        rc = lib.lf_stream_reserve_write(self._ptr, nbytes, out_p1, out_l1, out_p2, out_l2)
        CheckStatusError.check(rc, "StreamBuffer.reserve_write")
        v1 = _writable_view(out_p1[0], int(out_l1[0]))
        v2 = _writable_view(out_p2[0], int(out_l2[0]))
        res = StreamWriteReservation(self, v1, v2, nbytes)
        self._active_write_reservations.append(res)
        return res

    def commit_write(self, nbytes: int) -> None:
        """Commits ``nbytes`` previously reserved via ``reserve_write`` and releases active write views."""
        self._check_alive()
        if not isinstance(nbytes, int) or isinstance(nbytes, bool) or nbytes < 0:
            raise ValueError(f"nbytes must be a non-negative integer, got {nbytes}")
        while self._active_write_reservations:
            res = self._active_write_reservations[-1]
            res._committed = True
            res.release()
        if nbytes > 0:
            rc = lib.lf_stream_commit_write(self._ptr, nbytes)
            CheckStatusError.check(rc, "StreamBuffer.commit_write")

    def peek_read(self, max_bytes: int | None = None) -> StreamReadReservation:
        """Peeks at readable bytes as a zero-copy read-only ``(first_view, second_view)`` pair."""
        self._check_alive()
        if self._active_read_reservations:
            raise BufferError(
                "Cannot peek_read while an unconsumed read reservation is already active"
            )
        if max_bytes is not None and (
            not isinstance(max_bytes, int) or isinstance(max_bytes, bool) or max_bytes < 0
        ):
            raise ValueError(f"max_bytes must be a non-negative integer or None, got {max_bytes}")

        out_p1 = ffi.new("uint8_t**")
        out_l1 = ffi.new("size_t*")
        out_p2 = ffi.new("uint8_t**")
        out_l2 = ffi.new("size_t*")
        rc = lib.lf_stream_peek_read(self._ptr, out_p1, out_l1, out_p2, out_l2)
        CheckStatusError.check(rc, "StreamBuffer.peek_read")

        len1 = int(out_l1[0])
        len2 = int(out_l2[0])
        if max_bytes is not None:
            if len1 >= max_bytes:
                len1 = max_bytes
                len2 = 0
            elif len1 + len2 > max_bytes:
                len2 = max_bytes - len1

        v1 = _readonly_view(out_p1[0], len1)
        v2 = _readonly_view(out_p2[0], len2)
        res = StreamReadReservation(self, v1, v2, len1 + len2)
        self._active_read_reservations.append(res)
        return res

    def consume_read(self, nbytes: int) -> None:
        """Advances the read cursor by ``nbytes`` and releases active read views."""
        self._check_alive()
        if not isinstance(nbytes, int) or isinstance(nbytes, bool) or nbytes < 0:
            raise ValueError(f"nbytes must be a non-negative integer, got {nbytes}")
        while self._active_read_reservations:
            res = self._active_read_reservations[-1]
            res._consumed = True
            res.release()
        if nbytes > 0:
            rc = lib.lf_stream_consume_read(self._ptr, nbytes)
            CheckStatusError.check(rc, "StreamBuffer.consume_read")

    def write(self, data: bytes | bytearray | memoryview) -> int:
        """Writes up to ``len(data)`` bytes into the ring buffer, returning bytes written."""
        self._check_alive()
        view = memoryview(data).cast("B")
        n = len(view)
        if n == 0:
            return 0
        c_buf = ffi.from_buffer("uint8_t[]", view)
        return int(lib.lf_streambuffer_try_write(self._ptr, c_buf, n))

    def read(self, max_bytes: int | None = None) -> bytes:
        """Reads and consumes up to ``max_bytes`` (or all available bytes if ``None``)."""
        self._check_alive()
        avail = self.available_read
        target = avail if max_bytes is None else min(avail, max_bytes)
        if target < 0:
            raise ValueError("max_bytes must be non-negative")
        if target == 0:
            return b""
        out = bytearray(target)
        c_buf = ffi.from_buffer("uint8_t[]", out, require_writable=True)
        got = int(lib.lf_streambuffer_try_read(self._ptr, c_buf, target))
        if got == target:
            return bytes(out)
        return bytes(out[:got])

    def readinto(self, buf: bytearray | memoryview) -> int:
        """Reads directly into caller-provided writable buffer ``buf``, returning bytes read."""
        self._check_alive()
        view = memoryview(buf).cast("B")
        if view.readonly:
            raise TypeError("readinto requires a writable buffer")
        n = len(view)
        if n == 0:
            return 0
        c_buf = ffi.from_buffer("uint8_t[]", view, require_writable=True)
        return int(lib.lf_streambuffer_try_read(self._ptr, c_buf, n))

    def close(self) -> None:
        """Marks the stream buffer closed; raises ``BufferError`` if zero-copy views are still open."""
        if self._active_write_reservations or self._active_read_reservations:
            raise BufferError(
                "Cannot close StreamBuffer while zero-copy memoryview reservations are active"
            )
        self._closed = True

    def destroy(self) -> None:
        """Destroys the underlying C-ABI stream buffer; raises ``BufferError`` if views are active."""
        if self._active_write_reservations or self._active_read_reservations:
            raise BufferError(
                "Cannot destroy StreamBuffer while zero-copy memoryview reservations are active"
            )
        if not self._destroyed and self._ptr != ffi.NULL:
            ptr = self._ptr
            self._ptr = ffi.NULL
            self._destroyed = True
            self._closed = True
            lib.lf_streambuffer_destroy(ptr)

    def __del__(self) -> None:
        try:
            if not self._active_write_reservations and not self._active_read_reservations:
                self.destroy()
        except Exception:
            pass
