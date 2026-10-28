"""Sync + Async Channels (with 0-to-1 wakeup & Dekker fence, HIGH-02) and RendezvousChannel."""

from __future__ import annotations

import asyncio
import ctypes
import os
import threading
import time
import weakref
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import Any, Generic, TypeVar

from ._cabi import (
    LF_ERR_CLOSED,
    LF_ERR_EMPTY,
    LF_ERR_FULL,
    LF_OK,
    CheckStatusError,
    Closed,
    Empty,
    Full,
    box_pyobj,
    decref_pyobj_ptr,
    drain_deferred_decrefs,
    ffi,
    lib,
    unbox_pyobj_steal,
)
from .queues import BQueue128, MpmcQueue

T = TypeVar("T")

_MISSING = object()


def _load_rv_cdll() -> ctypes.CDLL:
    pkg_dir = Path(__file__).resolve().parent
    for name in ("liblockless_ext.dylib", "liblockless_ext.so", "lockless_ext.dll"):
        p = pkg_dir / name
        if p.exists():
            cdll = ctypes.CDLL(str(p))
            cdll.lf_rendezvous_send_timeout.argtypes = [
                ctypes.c_void_p,
                ctypes.c_void_p,
                ctypes.c_int32,
                ctypes.POINTER(ctypes.c_uint64),
            ]
            cdll.lf_rendezvous_send_timeout.restype = ctypes.c_int
            cdll.lf_rendezvous_recv_timeout.argtypes = [
                ctypes.c_void_p,
                ctypes.POINTER(ctypes.c_void_p),
                ctypes.c_int32,
                ctypes.POINTER(ctypes.c_uint64),
            ]
            cdll.lf_rendezvous_recv_timeout.restype = ctypes.c_int
            return cdll
    raise RuntimeError("Could not locate liblockless_ext for ctypes CDLL")


_rv_cdll = _load_rv_cdll()


class Channel(Generic[T]):
    """Bounded or unbounded channel supporting both synchronous and ``asyncio`` operations.

    Implements the ``HIGH-02`` 0-to-1 wakeup protocol with a Dekker-style store-load
    re-poll fence and an OS pipe registered with ``loop.add_reader`` so ``async for``
    and ``await ch.recv_async()`` never miss a wakeup and never touch the event loop
    on the non-empty fast path.

    Parameters
    ----------
    capacity : int | None, default 1024
        Power of two in ``[2, 65536]`` for a bounded channel, or ``None`` / ``0``
        for an unbounded channel.
    spsc : bool, default False
        When ``True`` and ``capacity`` is in ``[128, 16384]``, uses the wait-free
        128-bit batched ``BQueue128`` ring buffer as the underlying transport;
        otherwise uses ``MpmcQueue``.
    """

    __slots__ = (
        "__weakref__",
        "_async_read_waiters",
        "_async_write_waiters",
        "_capacity",
        "_closed",
        "_destroyed",
        "_lock",
        "_not_empty_cond",
        "_not_full_cond",
        "_queue",
        "_r_fd",
        "_registered_loops",
        "_spsc",
        "_w_fd",
        "_waiting_readers",
    )

    def __init__(self, capacity: int | None = 1024, *, spsc: bool = False) -> None:
        if spsc and capacity is not None and 128 <= capacity <= 16384:
            self._queue: BQueue128[T] | MpmcQueue[T] = BQueue128(capacity)
            self._capacity: int | None = capacity
        else:
            self._queue = MpmcQueue(capacity)
            self._capacity = self._queue.capacity

        self._spsc = spsc
        r_fd, w_fd = os.pipe()
        os.set_blocking(r_fd, False)
        os.set_blocking(w_fd, False)
        self._r_fd = r_fd
        self._w_fd = w_fd

        self._lock = threading.Lock()
        self._not_empty_cond = threading.Condition(self._lock)
        self._not_full_cond = threading.Condition(self._lock)
        self._waiting_readers = 0
        self._async_read_waiters: list[tuple[asyncio.AbstractEventLoop, asyncio.Future[None]]] = []
        self._async_write_waiters: list[tuple[asyncio.AbstractEventLoop, asyncio.Future[None]]] = []
        self._registered_loops: set[asyncio.AbstractEventLoop] = set()
        self._closed = False
        self._destroyed = False

    @property
    def capacity(self) -> int | None:
        return self._capacity

    @property
    def is_closed(self) -> bool:
        return self._closed or self._destroyed or self._queue.is_closed

    @property
    def is_empty(self) -> bool:
        return self._queue.is_empty

    @property
    def is_full(self) -> bool:
        return self._queue.is_full

    def __len__(self) -> int:
        return len(self._queue)

    def _signal_wakeup_pipe(self) -> None:
        if self._w_fd >= 0:
            try:
                os.write(self._w_fd, b"\x01")
            except BlockingIOError:
                pass
            except OSError:
                pass

    def _drain_wakeup_pipe(self) -> None:
        if self._r_fd >= 0:
            try:
                while os.read(self._r_fd, 64):
                    pass
            except BlockingIOError:
                pass
            except OSError:
                pass

    def _on_pipe_readable(self) -> None:
        self._drain_wakeup_pipe()
        self._wake_async_readers()

    def _wake_async_readers(self) -> None:
        with self._lock:
            waiters = list(self._async_read_waiters)
            self._async_read_waiters.clear()
        for loop, fut in waiters:
            if not fut.done():
                loop.call_soon_threadsafe(self._resolve_future, fut)

    def _wake_async_writers(self) -> None:
        with self._lock:
            waiters = list(self._async_write_waiters)
            self._async_write_waiters.clear()
        for loop, fut in waiters:
            if not fut.done():
                loop.call_soon_threadsafe(self._resolve_future, fut)

    @staticmethod
    def _resolve_future(fut: asyncio.Future[None]) -> None:
        if not fut.done():
            fut.set_result(None)

    @staticmethod
    def _weak_pipe_readable(self_ref: weakref.ReferenceType[Channel[Any]]) -> None:
        ch = self_ref()
        if ch is not None:
            ch._on_pipe_readable()

    def _ensure_loop_reader(self, loop: asyncio.AbstractEventLoop) -> None:
        if loop not in self._registered_loops and self._r_fd >= 0:
            try:
                loop.add_reader(self._r_fd, Channel._weak_pipe_readable, weakref.ref(self))
                self._registered_loops.add(loop)
            except Exception:
                pass

    def _remove_loop_readers(self) -> None:
        for loop in list(self._registered_loops):
            try:
                if not loop.is_closed() and self._r_fd >= 0:
                    loop.remove_reader(self._r_fd)
            except Exception:
                pass
        self._registered_loops.clear()

    def _notify_after_push(self, was_empty: bool) -> None:
        with self._lock:
            has_sync_waiters = was_empty or self._waiting_readers > 0
            has_async_waiters = len(self._async_read_waiters) > 0 or len(self._registered_loops) > 0
            if has_async_waiters and (was_empty or len(self._async_read_waiters) > 0):
                self._signal_wakeup_pipe()
            if has_sync_waiters:
                self._not_empty_cond.notify_all()
        if has_async_waiters:
            self._wake_async_readers()

    def _notify_after_pop(self) -> None:
        with self._lock:
            self._not_full_cond.notify_all()
            has_writers = len(self._async_write_waiters) > 0
        if has_writers:
            self._wake_async_writers()

    def send_nowait(self, item: T) -> None:
        """Enqueues ``item`` without blocking, raising ``Full`` or ``Closed``."""
        if self._closed or self._destroyed:
            raise Closed("Channel is closed")
        was_empty = self._queue.push_ex(item)
        self._notify_after_push(was_empty)

    def try_send(self, item: T) -> bool:
        """Attempts to enqueue ``item`` without blocking, returning ``False`` if full."""
        if self._closed or self._destroyed:
            raise Closed("Channel is closed")
        try:
            was_empty = self._queue.push_ex(item)
        except Full:
            return False
        self._notify_after_push(was_empty)
        return True

    def send(self, item: T, timeout: float | None = None) -> None:
        """Sends ``item`` synchronously, waiting if bounded channel is full."""
        deadline = None if timeout is None else (time.monotonic() + timeout)
        while True:
            if self._closed or self._destroyed:
                raise Closed("Channel is closed")
            try:
                was_empty = self._queue.push_ex(item)
                self._notify_after_push(was_empty)
                return
            except Full:
                with self._lock:
                    if self._closed or self._destroyed:
                        raise Closed("Channel is closed") from None
                    if not self._queue.is_full:
                        continue
                    if deadline is not None:
                        rem = deadline - time.monotonic()
                        if rem <= 0:
                            raise Full("Channel.send timed out while full") from None
                        self._not_full_cond.wait(timeout=min(rem, 0.005))
                    else:
                        self._not_full_cond.wait(timeout=0.005)

    def recv_nowait(self) -> T:
        """Dequeues and returns an item immediately, raising ``Empty`` or ``Closed``."""
        try:
            item = self._queue.pop()
            self._notify_after_pop()
            return item
        except Empty:
            if self.is_closed:
                raise Closed("Channel is closed and drained") from None
            raise

    def try_recv(self, default: Any = None) -> Any:
        """Dequeues and returns an item immediately, or ``default`` if empty."""
        try:
            return self.recv_nowait()
        except (Empty, Closed):
            return default

    def recv(self, timeout: float | None = None) -> T:
        """Receives the next item synchronously using the Dekker store-load wakeup fence."""
        deadline = None if timeout is None else (time.monotonic() + timeout)
        while True:
            # 1. Fast path: pop without lock
            try:
                item = self._queue.pop()
                self._notify_after_pop()
                return item
            except Empty:
                if self.is_closed and self._queue.is_empty:
                    raise Closed("Channel is closed and drained") from None

            # 2. Dekker fence: announce waiting reader, then re-poll queue before parking
            with self._lock:
                self._waiting_readers += 1
                try:
                    try:
                        item = self._queue.pop()
                        self._notify_after_pop_locked()
                        return item
                    except Empty:
                        if self.is_closed and self._queue.is_empty:
                            raise Closed("Channel is closed and drained") from None
                    if deadline is not None:
                        rem = deadline - time.monotonic()
                        if rem <= 0:
                            raise Empty("Channel.recv timed out") from None
                        self._not_empty_cond.wait(timeout=min(rem, 0.01))
                    else:
                        self._not_empty_cond.wait(timeout=0.01)
                finally:
                    self._waiting_readers -= 1

    def _notify_after_pop_locked(self) -> None:
        self._not_full_cond.notify_all()
        waiters = list(self._async_write_waiters)
        self._async_write_waiters.clear()
        for loop, fut in waiters:
            if not fut.done():
                loop.call_soon_threadsafe(self._resolve_future, fut)

    async def send_async(self, item: T) -> None:
        """Asynchronously sends ``item`` to the channel, awaiting space if full."""
        loop = asyncio.get_running_loop()
        while True:
            if self._closed or self._destroyed:
                raise Closed("Channel is closed")
            try:
                was_empty = self._queue.push_ex(item)
                self._notify_after_push(was_empty)
                return
            except Full:
                fut: asyncio.Future[None] = loop.create_future()
                with self._lock:
                    if self._closed or self._destroyed:
                        raise Closed("Channel is closed") from None
                    if not self._queue.is_full:
                        continue
                    self._async_write_waiters.append((loop, fut))
                await fut

    async def recv_async(self) -> T:
        """Asynchronously receives an item using the HIGH-02 0-to-1 pipe wakeup + Dekker fence."""
        loop = asyncio.get_running_loop()
        self._ensure_loop_reader(loop)
        while True:
            # 1. Zero-overhead fast path: drain directly if available
            try:
                item = self._queue.pop()
                self._notify_after_pop()
                return item
            except Empty:
                if self.is_closed and self._queue.is_empty:
                    raise Closed("Channel is closed and drained") from None

            # 2. Dekker store-load fence: register waiter, then re-poll before awaiting
            fut: asyncio.Future[None] = loop.create_future()
            with self._lock:
                self._waiting_readers += 1
                self._async_read_waiters.append((loop, fut))
                try:
                    item = self._queue.pop()
                    self._waiting_readers -= 1
                    try:
                        self._async_read_waiters.remove((loop, fut))
                    except ValueError:
                        pass
                    self._notify_after_pop_locked()
                    return item
                except Empty:
                    if self.is_closed and self._queue.is_empty:
                        self._waiting_readers -= 1
                        try:
                            self._async_read_waiters.remove((loop, fut))
                        except ValueError:
                            pass
                        raise Closed("Channel is closed and drained") from None

            try:
                await fut
            finally:
                with self._lock:
                    self._waiting_readers = max(0, self._waiting_readers - 1)
                    try:
                        self._async_read_waiters.remove((loop, fut))
                    except ValueError:
                        pass

    def __iter__(self) -> Iterator[T]:
        while True:
            try:
                yield self.recv()
            except Closed:
                return

    def __aiter__(self) -> AsyncIterator[T]:
        return self

    async def __anext__(self) -> T:
        try:
            return await self.recv_async()
        except Closed:
            raise StopAsyncIteration from None

    def close(self) -> None:
        """Closes the channel and wakes all blocked synchronous and async waiters."""
        if not self._closed:
            self._closed = True
            self._queue.close()
            with self._lock:
                if len(self._async_read_waiters) > 0 or len(self._registered_loops) > 0:
                    self._signal_wakeup_pipe()
                self._not_empty_cond.notify_all()
                self._not_full_cond.notify_all()
            self._wake_async_readers()
            self._wake_async_writers()
            self._remove_loop_readers()

    def destroy(self) -> None:
        self.close()
        if not self._destroyed:
            self._destroyed = True
            self._remove_loop_readers()
            if self._r_fd >= 0:
                try:
                    os.close(self._r_fd)
                except OSError:
                    pass
                self._r_fd = -1
            if self._w_fd >= 0:
                try:
                    os.close(self._w_fd)
                except OSError:
                    pass
                self._w_fd = -1
            self._queue.destroy()

    def __enter__(self) -> Channel[T]:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    def __del__(self) -> None:
        try:
            self.destroy()
        except Exception:
            pass


class RendezvousChannel(Generic[T]):
    """Zero-capacity synchronous handoff channel (`RendezvousChannel`).

    Every ``send`` blocks until a concurrent ``recv`` shakes hands directly (and vice versa).
    Releases the Python GIL during blocking handoffs via ``ctypes.CDLL`` timeout slices so
    concurrent Python threads rendezvous directly in C without GIL deadlocks.
    """

    __slots__ = ("_destroyed", "_ptr")

    def __init__(self) -> None:
        out = ffi.new("lf_rendezvous_t**")
        rc = lib.lf_rendezvous_create(out)
        CheckStatusError.check(rc, "RendezvousChannel.__init__")
        self._ptr = out[0]
        self._destroyed = False

    def _check_alive(self) -> None:
        if self._destroyed or self._ptr == ffi.NULL:
            raise Closed("RendezvousChannel has been destroyed")

    @property
    def is_closed(self) -> bool:
        if self._destroyed or self._ptr == ffi.NULL:
            return True
        return bool(lib.lf_rendezvous_is_closed(self._ptr))

    def try_send(self, item: T) -> bool:
        """Attempts an immediate handoff if a receiver is already waiting, else returns ``False``."""
        self._check_alive()
        raw = box_pyobj(item)
        corr = ffi.new("uint64_t*")
        rc = lib.lf_rendezvous_try_send(self._ptr, raw, corr)
        if rc == LF_OK:
            return True
        decref_pyobj_ptr(raw)
        if rc in (LF_ERR_EMPTY, LF_ERR_FULL):
            return False
        CheckStatusError.check(rc, "RendezvousChannel.try_send")
        return False

    def try_recv(self, default: Any = None) -> Any:
        """Attempts an immediate handoff if a sender is already waiting, else returns ``default``."""
        self._check_alive()
        out_payload = ffi.new("void**")
        corr = ffi.new("uint64_t*")
        rc = lib.lf_rendezvous_try_recv(self._ptr, out_payload, corr)
        if rc == LF_OK:
            return unbox_pyobj_steal(out_payload[0])
        if rc in (LF_ERR_EMPTY, LF_ERR_CLOSED):
            drain_deferred_decrefs()
            return default
        CheckStatusError.check(rc, "RendezvousChannel.try_recv")
        return default

    def send(self, item: T, timeout: float | None = None) -> int:
        """Hands off ``item`` to a matching ``recv`` call, returning the 64-bit correlation ID."""
        self._check_alive()
        if timeout is not None and timeout < 0:
            raise ValueError("timeout must be non-negative")
        raw = box_pyobj(item)
        raw_addr = int(ffi.cast("uintptr_t", raw))
        chan_addr = int(ffi.cast("uintptr_t", self._ptr))
        corr = ctypes.c_uint64(0)
        deadline = None if timeout is None else (time.monotonic() + timeout)

        while True:
            if self._destroyed or self._ptr == ffi.NULL:
                decref_pyobj_ptr(raw)
                raise Closed("RendezvousChannel has been destroyed")
            slice_ms = 20
            if deadline is not None:
                rem = deadline - time.monotonic()
                if rem <= 0:
                    decref_pyobj_ptr(raw)
                    raise TimeoutError("RendezvousChannel.send timed out")
                slice_ms = max(1, min(20, int(rem * 1000.0)))

            rc = int(
                _rv_cdll.lf_rendezvous_send_timeout(
                    chan_addr, raw_addr, slice_ms, ctypes.byref(corr)
                )
            )
            if rc == LF_OK:
                return int(corr.value)
            if rc == LF_ERR_CLOSED:
                decref_pyobj_ptr(raw)
                raise Closed("RendezvousChannel is closed")
            if rc not in (LF_ERR_EMPTY, LF_ERR_FULL):
                decref_pyobj_ptr(raw)
                CheckStatusError.check(rc, "RendezvousChannel.send")

            if deadline is not None and time.monotonic() >= deadline:
                decref_pyobj_ptr(raw)
                raise TimeoutError("RendezvousChannel.send timed out")

    def recv(self, timeout: float | None = None) -> T:
        """Receives an item directly from a matching ``send`` call."""
        self._check_alive()
        if timeout is not None and timeout < 0:
            raise ValueError("timeout must be non-negative")
        chan_addr = int(ffi.cast("uintptr_t", self._ptr))
        out_payload = ctypes.c_void_p(0)
        corr = ctypes.c_uint64(0)
        deadline = None if timeout is None else (time.monotonic() + timeout)

        while True:
            if self._destroyed or self._ptr == ffi.NULL:
                raise Closed("RendezvousChannel has been destroyed")
            slice_ms = 20
            if deadline is not None:
                rem = deadline - time.monotonic()
                if rem <= 0:
                    raise TimeoutError("RendezvousChannel.recv timed out")
                slice_ms = max(1, min(20, int(rem * 1000.0)))

            rc = int(
                _rv_cdll.lf_rendezvous_recv_timeout(
                    chan_addr, ctypes.byref(out_payload), slice_ms, ctypes.byref(corr)
                )
            )
            if rc == LF_OK:
                ptr_val = out_payload.value or 0
                return unbox_pyobj_steal(ffi.cast("void*", ptr_val))
            if rc == LF_ERR_CLOSED:
                raise Closed("RendezvousChannel is closed")
            if rc != LF_ERR_EMPTY:
                CheckStatusError.check(rc, "RendezvousChannel.recv")

            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError("RendezvousChannel.recv timed out")

    async def send_async(self, item: T, timeout: float | None = None) -> int:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self.send, item, timeout)

    async def recv_async(self, timeout: float | None = None) -> T:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self.recv, timeout)

    def close(self) -> None:
        if not self._destroyed and self._ptr != ffi.NULL:
            lib.lf_rendezvous_close(self._ptr)
            drain_deferred_decrefs()

    def destroy(self) -> None:
        if not self._destroyed and self._ptr != ffi.NULL:
            ptr = self._ptr
            self._ptr = ffi.NULL
            self._destroyed = True
            lib.lf_rendezvous_destroy(ptr)
            drain_deferred_decrefs()

    def __enter__(self) -> RendezvousChannel[T]:
        self._check_alive()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    def __del__(self) -> None:
        try:
            self.destroy()
        except Exception:
            pass
