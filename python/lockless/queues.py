"""Wait-free and lock-free SPSC / MPMC queues backed by the Nim C-ABI."""

from __future__ import annotations

import threading
import weakref
from typing import Any, Generic, TypeVar

from ._cabi import (
    LF_ERR_CLOSED,
    LF_ERR_EMPTY,
    LF_ERR_FULL,
    LF_OK,
    CheckStatusError,
    Closed,
    LocklessError,
    _pyobj_destructor_cb,
    box_pyobj,
    decref_pyobj_ptr,
    drain_deferred_decrefs,
    ffi,
    lib,
    unbox_pyobj_steal,
)

T = TypeVar("T")


def _is_power_of_two(n: int) -> bool:
    return n > 0 and (n & (n - 1)) == 0


class BQueue128(Generic[T]):
    """Wait-free batched SPSC ring buffer with 128-bit atomic batch probing.

    Capacity must be a power of two in ``[128, 16384]``.
    """

    __slots__ = ("_capacity", "_closed", "_destroyed", "_ptr")

    def __init__(self, capacity: int = 1024) -> None:
        if not isinstance(capacity, int) or isinstance(capacity, bool):
            raise TypeError("capacity must be an integer")
        if capacity < 128 or capacity > 16384 or not _is_power_of_two(capacity):
            raise ValueError(
                f"BQueue128 capacity must be a power of 2 in [128, 16384], got {capacity}"
            )
        out = ffi.new("lf_bqueue128_t**")
        rc = lib.lf_bqueue128_create(capacity, _pyobj_destructor_cb, ffi.NULL, out)
        if rc != LF_OK or out[0] == ffi.NULL:
            raise ValueError(f"Failed to create BQueue128 with capacity={capacity} (rc={rc})")
        self._ptr = out[0]
        self._capacity = capacity
        self._closed = False
        self._destroyed = False

    def _check_alive(self) -> None:
        if self._destroyed or self._ptr == ffi.NULL:
            raise Closed("BQueue128 has been destroyed")

    @property
    def capacity(self) -> int:
        return self._capacity

    @property
    def is_closed(self) -> bool:
        return self._destroyed or self._closed or self._ptr == ffi.NULL

    @property
    def is_empty(self) -> bool:
        self._check_alive()
        return bool(lib.lf_bqueue128_is_empty(self._ptr))

    @property
    def is_full(self) -> bool:
        self._check_alive()
        return bool(lib.lf_bqueue128_is_full(self._ptr))

    def __len__(self) -> int:
        self._check_alive()
        return int(lib.lf_bqueue128_len(self._ptr))

    def push(self, item: T) -> None:
        """Enqueues ``item`` without blocking, raising ``Full`` or ``Closed`` on failure."""
        self._check_alive()
        if self._closed:
            raise Closed("BQueue128 is closed")
        raw = box_pyobj(item)
        rc = lib.lf_bqueue128_push(self._ptr, raw)
        if rc != LF_OK:
            decref_pyobj_ptr(raw)
            CheckStatusError.check(rc, "BQueue128.push")

    def push_ex(self, item: T) -> bool:
        """Enqueues ``item`` and returns ``True`` if the queue transitioned from 0 to 1 item."""
        self._check_alive()
        if self._closed:
            raise Closed("BQueue128 is closed")
        raw = box_pyobj(item)
        was_empty = ffi.new("bool*")
        rc = lib.lf_bqueue128_push_ex(self._ptr, raw, was_empty)
        if rc != LF_OK:
            decref_pyobj_ptr(raw)
            CheckStatusError.check(rc, "BQueue128.push_ex")
        return bool(was_empty[0])

    def try_push(self, item: T) -> bool:
        """Attempts to enqueue ``item``, returning ``False`` if full or raising ``Closed``."""
        self._check_alive()
        if self._closed:
            raise Closed("BQueue128 is closed")
        raw = box_pyobj(item)
        rc = lib.lf_bqueue128_push(self._ptr, raw)
        if rc == LF_OK:
            return True
        decref_pyobj_ptr(raw)
        if rc == LF_ERR_FULL:
            return False
        CheckStatusError.check(rc, "BQueue128.try_push")
        return False

    put_nowait = push

    def pop(self) -> T:
        """Dequeues and returns the head item, raising ``Empty`` or ``Closed``."""
        self._check_alive()
        out_item = ffi.new("void**")
        rc = lib.lf_bqueue128_pop(self._ptr, out_item)
        if rc != LF_OK:
            if rc == LF_ERR_EMPTY and self._closed:
                raise Closed("BQueue128 is closed")
            CheckStatusError.check(rc, "BQueue128.pop")
        return unbox_pyobj_steal(out_item[0])

    get_nowait = pop

    def try_pop(self, default: Any = None) -> Any:
        """Dequeues and returns the head item, or ``default`` if empty."""
        self._check_alive()
        out_item = ffi.new("void**")
        rc = lib.lf_bqueue128_pop(self._ptr, out_item)
        if rc == LF_OK:
            return unbox_pyobj_steal(out_item[0])
        if rc in (LF_ERR_EMPTY, LF_ERR_CLOSED):
            drain_deferred_decrefs()
            return default
        CheckStatusError.check(rc, "BQueue128.try_pop")
        return default

    def pop_batch(self, max_count: int = 64) -> list[T]:
        """Dequeues up to ``max_count`` items."""
        self._check_alive()
        if max_count <= 0:
            raise ValueError("max_count must be positive")
        out_item = ffi.new("void**")
        result: list[T] = []
        for _ in range(max_count):
            rc = lib.lf_bqueue128_pop(self._ptr, out_item)
            if rc != LF_OK:
                break
            result.append(unbox_pyobj_steal(out_item[0]))
        drain_deferred_decrefs()
        return result

    def clear(self) -> None:
        """Drains all buffered items and releases their Python reference counts."""
        self._check_alive()
        lib.lf_bqueue128_clear(self._ptr)
        drain_deferred_decrefs()

    def close(self) -> None:
        """Closes the queue to further pushes while allowing remaining items to be popped."""
        if not self._destroyed and self._ptr != ffi.NULL:
            self._closed = True
            drain_deferred_decrefs()

    def destroy(self) -> None:
        """Destroys the underlying C-ABI queue and reclaims all remaining items."""
        if not self._destroyed and self._ptr != ffi.NULL:
            ptr = self._ptr
            self._ptr = ffi.NULL
            self._closed = True
            self._destroyed = True
            lib.lf_bqueue128_destroy(ptr)
            drain_deferred_decrefs()

    def __enter__(self) -> BQueue128[T]:
        self._check_alive()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    def __del__(self) -> None:
        try:
            self.destroy()
        except Exception:
            pass


class SpscQueue(Generic[T]):
    """Wait-free Lamport Single-Producer Single-Consumer ring buffer.

    Capacity must be a power of two in ``[2, 65536]``.
    """

    __slots__ = ("_capacity", "_destroyed", "_ptr")

    def __init__(self, capacity: int = 1024) -> None:
        if not isinstance(capacity, int) or isinstance(capacity, bool):
            raise TypeError("capacity must be an integer")
        if capacity < 2 or capacity > 65536 or not _is_power_of_two(capacity):
            raise ValueError(
                f"SpscQueue capacity must be a power of 2 in [2, 65536], got {capacity}"
            )
        out = ffi.new("ll_spsc_t**")
        rc = lib.ll_spsc_create(capacity, out)
        if rc != LF_OK or out[0] == ffi.NULL:
            raise ValueError(f"Failed to create SpscQueue with capacity={capacity} (rc={rc})")
        self._ptr = out[0]
        self._capacity = capacity
        self._destroyed = False

    def _check_alive(self) -> None:
        if self._destroyed or self._ptr == ffi.NULL:
            raise Closed("SpscQueue has been destroyed")

    @property
    def capacity(self) -> int:
        return self._capacity

    @property
    def is_closed(self) -> bool:
        if self._destroyed or self._ptr == ffi.NULL:
            return True
        return bool(lib.ll_spsc_is_closed(self._ptr))

    @property
    def is_empty(self) -> bool:
        self._check_alive()
        return bool(lib.ll_spsc_is_empty(self._ptr))

    @property
    def is_full(self) -> bool:
        self._check_alive()
        return bool(lib.ll_spsc_is_full(self._ptr))

    def __len__(self) -> int:
        self._check_alive()
        return int(lib.ll_spsc_len(self._ptr))

    def push(self, item: T) -> None:
        """Enqueues ``item`` without blocking, raising ``Full`` or ``Closed``."""
        self._check_alive()
        raw = box_pyobj(item)
        rc = lib.ll_spsc_push(self._ptr, raw)
        if rc != LF_OK:
            decref_pyobj_ptr(raw)
            CheckStatusError.check(rc, "SpscQueue.push")

    def try_push(self, item: T) -> bool:
        """Attempts to enqueue ``item``, returning ``False`` if full."""
        self._check_alive()
        raw = box_pyobj(item)
        rc = lib.ll_spsc_push(self._ptr, raw)
        if rc == LF_OK:
            return True
        decref_pyobj_ptr(raw)
        if rc == LF_ERR_FULL:
            return False
        CheckStatusError.check(rc, "SpscQueue.try_push")
        return False

    put_nowait = push

    def pop(self) -> T:
        """Dequeues and returns the head item, raising ``Empty`` or ``Closed``."""
        self._check_alive()
        out_item = ffi.new("void**")
        rc = lib.ll_spsc_pop(self._ptr, out_item)
        if rc != LF_OK:
            CheckStatusError.check(rc, "SpscQueue.pop")
        return unbox_pyobj_steal(out_item[0])

    get_nowait = pop

    def try_pop(self, default: Any = None) -> Any:
        """Dequeues and returns the head item, or ``default`` if empty."""
        self._check_alive()
        out_item = ffi.new("void**")
        rc = lib.ll_spsc_pop(self._ptr, out_item)
        if rc == LF_OK:
            return unbox_pyobj_steal(out_item[0])
        if rc in (LF_ERR_EMPTY, LF_ERR_CLOSED):
            drain_deferred_decrefs()
            return default
        CheckStatusError.check(rc, "SpscQueue.try_pop")
        return default

    def pop_batch(self, max_count: int = 64) -> list[T]:
        """Dequeues up to ``max_count`` items in a single C-ABI batch call."""
        self._check_alive()
        if max_count <= 0:
            raise ValueError("max_count must be positive")
        buf = ffi.new("void*[]", max_count)
        n = int(lib.ll_spsc_pop_batch(self._ptr, buf, max_count))
        result: list[T] = [unbox_pyobj_steal(buf[i]) for i in range(n)]
        drain_deferred_decrefs()
        return result

    def clear(self) -> None:
        """Drains all buffered items and releases their Python reference counts."""
        self._check_alive()
        lib.ll_spsc_clear(self._ptr)
        drain_deferred_decrefs()

    def close(self) -> None:
        """Closes the queue to further pushes."""
        if not self._destroyed and self._ptr != ffi.NULL:
            lib.ll_spsc_close(self._ptr)
            drain_deferred_decrefs()

    def destroy(self) -> None:
        """Destroys the underlying C-ABI queue and reclaims all remaining items."""
        if not self._destroyed and self._ptr != ffi.NULL:
            ptr = self._ptr
            self._ptr = ffi.NULL
            self._destroyed = True
            lib.ll_spsc_destroy(ptr)
            drain_deferred_decrefs()

    def __enter__(self) -> SpscQueue[T]:
        self._check_alive()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    def __del__(self) -> None:
        try:
            self.destroy()
        except Exception:
            pass


class MpmcProducer(Generic[T]):
    """Producer handle for an ``MpmcQueue``."""

    __slots__ = ("__weakref__", "_closed", "_ptr", "_queue")

    def __init__(self, queue: MpmcQueue[T], ptr: Any) -> None:
        self._queue = queue
        self._ptr = ptr
        self._closed = False

    def push(self, item: T) -> None:
        if self._closed or self._ptr == ffi.NULL or self._queue._destroyed:
            raise Closed("MpmcProducer is closed or MpmcQueue is destroyed")
        raw = box_pyobj(item)
        rc = lib.lf_push(self._ptr, raw)
        if rc != LF_OK:
            decref_pyobj_ptr(raw)
            CheckStatusError.check(rc, "MpmcProducer.push")

    def push_ex(self, item: T) -> bool:
        if self._closed or self._ptr == ffi.NULL or self._queue._destroyed:
            raise Closed("MpmcProducer is closed or MpmcQueue is destroyed")
        raw = box_pyobj(item)
        was_empty = ffi.new("bool*")
        rc = lib.lf_push_ex(self._ptr, raw, was_empty)
        if rc != LF_OK:
            decref_pyobj_ptr(raw)
            CheckStatusError.check(rc, "MpmcProducer.push_ex")
        return bool(was_empty[0])

    def try_push(self, item: T) -> bool:
        if self._closed or self._ptr == ffi.NULL or self._queue._destroyed:
            raise Closed("MpmcProducer is closed or MpmcQueue is destroyed")
        raw = box_pyobj(item)
        rc = lib.lf_push(self._ptr, raw)
        if rc == LF_OK:
            return True
        decref_pyobj_ptr(raw)
        if rc == LF_ERR_FULL:
            return False
        CheckStatusError.check(rc, "MpmcProducer.try_push")
        return False

    def close(self) -> None:
        if not self._closed and self._ptr != ffi.NULL:
            ptr = self._ptr
            self._ptr = ffi.NULL
            self._closed = True
            lib.lf_producer_release(ptr)

    def __enter__(self) -> MpmcProducer[T]:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass


class MpmcConsumer(Generic[T]):
    """Consumer handle for an ``MpmcQueue``."""

    __slots__ = ("__weakref__", "_closed", "_ptr", "_queue")

    def __init__(self, queue: MpmcQueue[T], ptr: Any) -> None:
        self._queue = queue
        self._ptr = ptr
        self._closed = False

    def pop(self) -> T:
        if self._closed or self._ptr == ffi.NULL or self._queue._destroyed:
            raise Closed("MpmcConsumer is closed or MpmcQueue is destroyed")
        out_item = ffi.new("void**")
        rc = lib.lf_pop(self._ptr, out_item)
        if rc != LF_OK:
            CheckStatusError.check(rc, "MpmcConsumer.pop")
        return unbox_pyobj_steal(out_item[0])

    def try_pop(self, default: Any = None) -> Any:
        if self._closed or self._ptr == ffi.NULL or self._queue._destroyed:
            raise Closed("MpmcConsumer is closed or MpmcQueue is destroyed")
        out_item = ffi.new("void**")
        rc = lib.lf_pop(self._ptr, out_item)
        if rc == LF_OK:
            return unbox_pyobj_steal(out_item[0])
        if rc in (LF_ERR_EMPTY, LF_ERR_CLOSED):
            drain_deferred_decrefs()
            return default
        CheckStatusError.check(rc, "MpmcConsumer.try_pop")
        return default

    def pop_batch(self, max_count: int = 64) -> list[T]:
        if self._closed or self._ptr == ffi.NULL or self._queue._destroyed:
            raise Closed("MpmcConsumer is closed or MpmcQueue is destroyed")
        if max_count <= 0:
            raise ValueError("max_count must be positive")
        buf = ffi.new("void*[]", max_count)
        n = int(lib.lf_pop_batch(self._ptr, buf, max_count))
        result: list[T] = [unbox_pyobj_steal(buf[i]) for i in range(n)]
        drain_deferred_decrefs()
        return result

    def close(self) -> None:
        if not self._closed and self._ptr != ffi.NULL:
            ptr = self._ptr
            self._ptr = ffi.NULL
            self._closed = True
            lib.lf_consumer_release(ptr)

    def __enter__(self) -> MpmcConsumer[T]:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass


class MpmcQueue(Generic[T]):
    """Lock-free Multi-Producer Multi-Consumer queue (Vyukov bounded or Michael-Scott unbounded).

    Parameters
    ----------
    capacity : int | None, default 1024
        Power of two in ``[2, 65536]`` for a bounded Vyukov MPMC ring, or
        ``None`` / ``0`` for an unbounded Michael-Scott MPMC queue with Debra SMR.
    """

    __slots__ = (
        "_capacity",
        "_consumers",
        "_default_cons",
        "_default_prod",
        "_destroyed",
        "_producers",
        "_ptr",
        "_tls_cons_ptrs",
        "_tls_handles",
        "_tls_prod_ptrs",
    )

    def __init__(self, capacity: int | None = 1024) -> None:
        out_q = ffi.new("lf_queue_t**")
        if capacity is None or (
            isinstance(capacity, int) and not isinstance(capacity, bool) and capacity == 0
        ):
            rc = lib.lf_unbounded_mpmc_create(0, 0, _pyobj_destructor_cb, ffi.NULL, out_q)
            if rc != LF_OK or out_q[0] == ffi.NULL:
                raise LocklessError(f"Failed to create unbounded MpmcQueue (rc={rc})")
            self._capacity: int | None = None
        else:
            if not isinstance(capacity, int) or isinstance(capacity, bool):
                raise TypeError("capacity must be an integer or None")
            if capacity < 2 or capacity > 65536 or not _is_power_of_two(capacity):
                raise ValueError(
                    f"Bounded MpmcQueue capacity must be a power of 2 in [2, 65536], got {capacity}"
                )
            rc = lib.lf_bounded_mpmc_create(capacity, 0, 0, _pyobj_destructor_cb, ffi.NULL, out_q)
            if rc != LF_OK or out_q[0] == ffi.NULL:
                raise ValueError(
                    f"Failed to create bounded MpmcQueue with capacity={capacity} (rc={rc})"
                )
            self._capacity = capacity

        self._ptr = out_q[0]
        self._destroyed = False
        self._producers: weakref.WeakSet[MpmcProducer[T]] = weakref.WeakSet()
        self._consumers: weakref.WeakSet[MpmcConsumer[T]] = weakref.WeakSet()
        self._tls_handles = threading.local()
        self._tls_prod_ptrs: list[Any] = []
        self._tls_cons_ptrs: list[Any] = []

        if self._capacity is not None:
            out_p = ffi.new("lf_producer_t**")
            rc_p = lib.lf_producer_acquire(self._ptr, out_p)
            if rc_p != LF_OK:
                lib.lf_queue_destroy(self._ptr)
                self._ptr = ffi.NULL
                self._destroyed = True
                CheckStatusError.check(rc_p, "MpmcQueue.producer")
            self._default_prod = out_p[0]

            out_c = ffi.new("lf_consumer_t**")
            rc_c = lib.lf_consumer_acquire(self._ptr, out_c)
            if rc_c != LF_OK:
                lib.lf_producer_release(self._default_prod)
                self._default_prod = ffi.NULL
                lib.lf_queue_destroy(self._ptr)
                self._ptr = ffi.NULL
                self._destroyed = True
                CheckStatusError.check(rc_c, "MpmcQueue.consumer")
            self._default_cons = out_c[0]
        else:
            self._default_prod = ffi.NULL
            self._default_cons = ffi.NULL

    def _check_alive(self) -> None:
        if self._destroyed or self._ptr == ffi.NULL:
            raise Closed("MpmcQueue has been destroyed")

    def _get_default_prod(self) -> Any:
        if self._capacity is not None:
            return self._default_prod
        p = getattr(self._tls_handles, "prod", ffi.NULL)
        if p == ffi.NULL:
            out_p = ffi.new("lf_producer_t**")
            rc = lib.lf_producer_acquire(self._ptr, out_p)
            CheckStatusError.check(rc, "MpmcQueue.producer")
            p = out_p[0]
            self._tls_handles.prod = p
            self._tls_prod_ptrs.append(p)
        return p

    def _get_default_cons(self) -> Any:
        if self._capacity is not None:
            return self._default_cons
        c = getattr(self._tls_handles, "cons", ffi.NULL)
        if c == ffi.NULL:
            out_c = ffi.new("lf_consumer_t**")
            rc = lib.lf_consumer_acquire(self._ptr, out_c)
            CheckStatusError.check(rc, "MpmcQueue.consumer")
            c = out_c[0]
            self._tls_handles.cons = c
            self._tls_cons_ptrs.append(c)
        return c

    @property
    def capacity(self) -> int | None:
        return self._capacity

    @property
    def is_closed(self) -> bool:
        if self._destroyed or self._ptr == ffi.NULL:
            return True
        return bool(lib.lf_queue_is_closed(self._ptr))

    @property
    def is_empty(self) -> bool:
        self._check_alive()
        return bool(lib.lf_queue_is_empty(self._ptr))

    @property
    def is_full(self) -> bool:
        self._check_alive()
        if self._capacity is None:
            return False
        return len(self) >= self._capacity

    def __len__(self) -> int:
        self._check_alive()
        return int(lib.lf_queue_len(self._ptr))

    def producer(self) -> MpmcProducer[T]:
        """Creates an independent producer handle for this queue."""
        self._check_alive()
        out_p = ffi.new("lf_producer_t**")
        rc = lib.lf_producer_acquire(self._ptr, out_p)
        CheckStatusError.check(rc, "MpmcQueue.producer")
        prod = MpmcProducer(self, out_p[0])
        self._producers.add(prod)
        return prod

    def consumer(self) -> MpmcConsumer[T]:
        """Creates an independent consumer handle for this queue."""
        self._check_alive()
        out_c = ffi.new("lf_consumer_t**")
        rc = lib.lf_consumer_acquire(self._ptr, out_c)
        CheckStatusError.check(rc, "MpmcQueue.consumer")
        cons = MpmcConsumer(self, out_c[0])
        self._consumers.add(cons)
        return cons

    def push(self, item: T) -> None:
        """Enqueues ``item`` without blocking, raising ``Full`` or ``Closed``."""
        self._check_alive()
        prod = self._get_default_prod()
        raw = box_pyobj(item)
        rc = lib.lf_push(prod, raw)
        if rc != LF_OK:
            decref_pyobj_ptr(raw)
            CheckStatusError.check(rc, "MpmcQueue.push")

    def push_ex(self, item: T) -> bool:
        """Enqueues ``item`` and returns ``True`` if the queue transitioned from 0 to 1 item."""
        self._check_alive()
        prod = self._get_default_prod()
        raw = box_pyobj(item)
        was_empty = ffi.new("bool*")
        rc = lib.lf_push_ex(prod, raw, was_empty)
        if rc != LF_OK:
            decref_pyobj_ptr(raw)
            CheckStatusError.check(rc, "MpmcQueue.push_ex")
        return bool(was_empty[0])

    def try_push(self, item: T) -> bool:
        """Attempts to enqueue ``item``, returning ``False`` if full."""
        self._check_alive()
        prod = self._get_default_prod()
        raw = box_pyobj(item)
        rc = lib.lf_push(prod, raw)
        if rc == LF_OK:
            return True
        decref_pyobj_ptr(raw)
        if rc == LF_ERR_FULL:
            return False
        CheckStatusError.check(rc, "MpmcQueue.try_push")
        return False

    put_nowait = push

    def pop(self) -> T:
        """Dequeues and returns an item, raising ``Empty`` or ``Closed``."""
        self._check_alive()
        cons = self._get_default_cons()
        out_item = ffi.new("void**")
        rc = lib.lf_pop(cons, out_item)
        if rc != LF_OK:
            CheckStatusError.check(rc, "MpmcQueue.pop")
        return unbox_pyobj_steal(out_item[0])

    get_nowait = pop

    def try_pop(self, default: Any = None) -> Any:
        """Dequeues and returns an item, or ``default`` if empty."""
        self._check_alive()
        cons = self._get_default_cons()
        out_item = ffi.new("void**")
        rc = lib.lf_pop(cons, out_item)
        if rc == LF_OK:
            return unbox_pyobj_steal(out_item[0])
        if rc in (LF_ERR_EMPTY, LF_ERR_CLOSED):
            drain_deferred_decrefs()
            return default
        CheckStatusError.check(rc, "MpmcQueue.try_pop")
        return default

    def pop_batch(self, max_count: int = 64) -> list[T]:
        """Dequeues up to ``max_count`` items in a single C-ABI batch call."""
        self._check_alive()
        if max_count <= 0:
            raise ValueError("max_count must be positive")
        cons = self._get_default_cons()
        buf = ffi.new("void*[]", max_count)
        n = int(lib.lf_pop_batch(cons, buf, max_count))
        result: list[T] = [unbox_pyobj_steal(buf[i]) for i in range(n)]
        drain_deferred_decrefs()
        return result

    def clear(self) -> None:
        """Drains all items currently in the queue and releases their Python reference counts."""
        self._check_alive()
        cons = self._get_default_cons()
        out_item = ffi.new("void**")
        while lib.lf_pop(cons, out_item) == LF_OK:
            unbox_pyobj_steal(out_item[0])
        drain_deferred_decrefs()

    def close(self) -> None:
        """Closes the queue to further pushes."""
        if not self._destroyed and self._ptr != ffi.NULL:
            lib.lf_queue_close(self._ptr)
            drain_deferred_decrefs()

    def destroy(self) -> None:
        """Closes all open handles and destroys the underlying C-ABI queue."""
        if not self._destroyed and self._ptr != ffi.NULL:
            for prod in list(self._producers):
                prod.close()
            self._producers.clear()
            for cons in list(self._consumers):
                cons.close()
            self._consumers.clear()
            for p_ptr in self._tls_prod_ptrs:
                if p_ptr != ffi.NULL:
                    lib.lf_producer_release(p_ptr)
            self._tls_prod_ptrs.clear()
            for c_ptr in self._tls_cons_ptrs:
                if c_ptr != ffi.NULL:
                    lib.lf_consumer_release(c_ptr)
            self._tls_cons_ptrs.clear()
            if self._default_prod != ffi.NULL:
                lib.lf_producer_release(self._default_prod)
                self._default_prod = ffi.NULL
            if self._default_cons != ffi.NULL:
                lib.lf_consumer_release(self._default_cons)
                self._default_cons = ffi.NULL
            self._destroyed = True
            ptr = self._ptr
            self._ptr = ffi.NULL
            lib.lf_queue_destroy(ptr)
            drain_deferred_decrefs()

    def __enter__(self) -> MpmcQueue[T]:
        self._check_alive()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    def __del__(self) -> None:
        try:
            self.destroy()
        except Exception:
            pass
