"""Lock-free BroadcastRing, ConcurrentDict (Ctrie), SkipListMap, and SkipListSet (HIGH-03)."""

from __future__ import annotations

import time
import weakref
from collections.abc import Callable, Iterator
from typing import Any, Generic, TypeVar

from ._cabi import (
    LF_ERR_EMPTY,
    LF_ERR_FULL,
    LF_ERR_REGISTRY_FULL,
    LF_OK,
    LF_OVERFLOW_BACKPRESSURE,
    LF_OVERFLOW_DROP_OLDEST,
    LF_POLL_LAGGED,
    LF_POLL_SUCCESS,
    LF_SUB_FROM_EARLIEST,
    LF_SUB_FROM_LATEST,
    CheckStatusError,
    Closed,
    Empty,
    Full,
    LocklessError,
    box_pyobj,
    decref_pyobj_ptr,
    drain_deferred_decrefs,
    encode_key,
    enter_borrow,
    exit_borrow,
    ffi,
    lib,
    unbox_pyobj_borrow,
)

K = TypeVar("K")
V = TypeVar("V")
T = TypeVar("T")

_MISSING = object()


def _is_power_of_two(n: int) -> bool:
    return n > 0 and (n & (n - 1)) == 0


class LaggedError(LocklessError):
    """Raised when a ``BroadcastRing`` subscriber lags behind overwritten ring slots."""

    def __init__(self, skipped_count: int) -> None:
        super().__init__(f"Subscriber lagged by {skipped_count} overwritten messages")
        self.skipped_count = skipped_count


class Subscriber(Generic[T]):
    """Independent reader cursor on a ``BroadcastRing``."""

    __slots__ = ("__weakref__", "_closed", "_ptr", "_ring", "_total_skipped")

    def __init__(self, ring: BroadcastRing[T], ptr: Any) -> None:
        self._ring = ring
        self._ptr = ptr
        self._closed = False
        self._total_skipped = 0

    @property
    def is_closed(self) -> bool:
        return self._closed or self._ptr == ffi.NULL or self._ring._destroyed

    @property
    def lag(self) -> int:
        if self._closed or self._ptr == ffi.NULL or self._ring._destroyed:
            return 0
        return int(lib.ll_broadcast_cursor_lag(self._ptr))

    @property
    def total_skipped(self) -> int:
        return self._total_skipped

    def poll(self, *, raise_on_lag: bool = False) -> T:
        """Polls the next message for this subscriber.

        If the subscriber lagged behind overwritten slots in ``drop_oldest`` mode,
        ``total_skipped`` is incremented by the number of skipped messages; if
        ``raise_on_lag=True``, raises ``LaggedError(skipped_count)``, otherwise
        transparently fast-forwards and returns the earliest available message.
        """
        if self._closed or self._ptr == ffi.NULL or self._ring._destroyed:
            raise Closed("Subscriber is unsubscribed or BroadcastRing is destroyed")
        out_item = ffi.new("void**")
        out_skipped = ffi.new("size_t*")
        out_res = ffi.new("lf_poll_result_t*")

        while True:
            if self._closed or self._ptr == ffi.NULL or self._ring._destroyed:
                raise Closed("Subscriber is unsubscribed or BroadcastRing is destroyed")
            enter_borrow()
            try:
                rc = lib.ll_broadcast_poll(self._ptr, out_item, out_skipped, out_res)
                if rc == LF_OK:
                    kind = int(out_res[0])
                    if kind == LF_POLL_LAGGED:
                        skipped = int(out_skipped[0])
                        self._total_skipped += skipped
                        if raise_on_lag:
                            raise LaggedError(skipped)
                        continue
                    if kind == LF_POLL_SUCCESS:
                        return unbox_pyobj_borrow(out_item[0])
                CheckStatusError.check(rc, "Subscriber.poll")
            finally:
                exit_borrow()

    def try_poll(self, default: Any = None) -> Any:
        """Polls the next message, returning ``default`` if no new message is available."""
        try:
            return self.poll(raise_on_lag=False)
        except (Empty, Closed):
            return default

    def recv(self, timeout: float | None = None) -> T:
        """Waits for and returns the next broadcast message, or raises ``TimeoutError`` / ``Closed``."""
        deadline = None if timeout is None else (time.monotonic() + timeout)
        sleep_s = 0.00005
        while True:
            try:
                return self.poll(raise_on_lag=False)
            except Empty:
                if self._ring.is_closed:
                    raise Closed("BroadcastRing is closed and drained") from None
                if deadline is not None and time.monotonic() >= deadline:
                    raise TimeoutError("Timed out waiting for BroadcastRing message") from None
                time.sleep(sleep_s)
                if sleep_s < 0.001:
                    sleep_s *= 2.0

    def unsubscribe(self) -> None:
        """Detaches this subscriber cursor from the ``BroadcastRing``."""
        if not self._closed and self._ptr != ffi.NULL:
            ptr = self._ptr
            self._ptr = ffi.NULL
            self._closed = True
            if not self._ring._destroyed and self._ring._ptr != ffi.NULL:
                lib.ll_broadcast_unsubscribe(ptr)
            drain_deferred_decrefs()

    close = unsubscribe

    def __enter__(self) -> Subscriber[T]:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.unsubscribe()

    def __del__(self) -> None:
        try:
            self.unsubscribe()
        except Exception:
            pass


BroadcastSubscriber = Subscriber


class BroadcastRing(Generic[T]):
    """Wait-free / lock-free pub-sub ring buffer where every subscriber receives every message.

    Parameters
    ----------
    capacity : int, default 1024
        Power of two in ``[2, 65536]``.
    overflow : str, default "drop_oldest"
        Either ``"drop_oldest"`` (wait-free overwrite of oldest unread slot) or
        ``"backpressure"`` / ``"reject"`` (never overwrite unread slots).
    max_readers : int, default 16
        Maximum concurrent subscribers in ``[1, 64]``.
    """

    __slots__ = ("_capacity", "_destroyed", "_max_readers", "_overflow", "_ptr", "_subscribers")

    def __init__(
        self,
        capacity: int = 1024,
        overflow: str = "drop_oldest",
        max_readers: int = 16,
    ) -> None:
        if not isinstance(capacity, int) or isinstance(capacity, bool):
            raise TypeError("capacity must be an integer")
        if capacity < 16 or capacity > 65536 or not _is_power_of_two(capacity):
            raise ValueError(
                f"BroadcastRing capacity must be a power of 2 in [16, 65536], got {capacity}"
            )
        if (
            not isinstance(max_readers, int)
            or isinstance(max_readers, bool)
            or not (1 <= max_readers <= 64)
        ):
            raise ValueError(f"max_readers must be an integer in [1, 64], got {max_readers}")

        norm_overflow = overflow.lower().strip()
        if norm_overflow in ("drop_oldest", "overwrite"):
            mode = LF_OVERFLOW_DROP_OLDEST
        elif norm_overflow in ("backpressure", "reject"):
            mode = LF_OVERFLOW_BACKPRESSURE
        else:
            raise ValueError(
                f"Invalid overflow mode {overflow!r}; expected 'drop_oldest' or 'backpressure'"
            )

        out = ffi.new("ll_broadcast_t**")
        rc = lib.ll_broadcast_create(capacity, mode, max_readers, out)
        if rc != LF_OK or out[0] == ffi.NULL:
            raise ValueError(f"Failed to create BroadcastRing (rc={rc})")
        self._ptr = out[0]
        self._capacity = capacity
        self._overflow = norm_overflow
        self._max_readers = max_readers
        self._destroyed = False
        self._subscribers: weakref.WeakSet[Subscriber[T]] = weakref.WeakSet()

    def _check_alive(self) -> None:
        if self._destroyed or self._ptr == ffi.NULL:
            raise Closed("BroadcastRing has been destroyed")

    @property
    def capacity(self) -> int:
        return self._capacity

    @property
    def subscriber_count(self) -> int:
        self._check_alive()
        return int(lib.ll_broadcast_subscriber_count(self._ptr))

    @property
    def is_closed(self) -> bool:
        if self._destroyed or self._ptr == ffi.NULL:
            return True
        return bool(lib.ll_broadcast_is_closed(self._ptr))

    def __len__(self) -> int:
        self._check_alive()
        return int(lib.ll_broadcast_len(self._ptr))

    def publish(self, item: T) -> None:
        """Publishes ``item`` to all active subscribers."""
        self._check_alive()
        raw = box_pyobj(item)
        rc = lib.ll_broadcast_publish(self._ptr, raw)
        if rc != LF_OK:
            decref_pyobj_ptr(raw)
            CheckStatusError.check(rc, "BroadcastRing.publish")
        drain_deferred_decrefs()

    def try_publish(self, item: T) -> bool:
        """Attempts to publish ``item`` without blocking, returning ``False`` if full."""
        self._check_alive()
        raw = box_pyobj(item)
        rc = lib.ll_broadcast_try_publish(self._ptr, raw)
        if rc == LF_OK:
            drain_deferred_decrefs()
            return True
        decref_pyobj_ptr(raw)
        if rc == LF_ERR_FULL:
            return False
        CheckStatusError.check(rc, "BroadcastRing.try_publish")
        return False

    def subscribe(self, *, from_earliest: bool = False) -> Subscriber[T]:
        """Creates and attaches a new ``Subscriber`` cursor."""
        self._check_alive()
        origin = LF_SUB_FROM_EARLIEST if from_earliest else LF_SUB_FROM_LATEST
        out_cur = ffi.new("ll_broadcast_cursor_t**")
        rc = lib.ll_broadcast_subscribe(self._ptr, origin, out_cur)
        if rc == LF_ERR_REGISTRY_FULL:
            raise Full(f"BroadcastRing max_readers ({self._max_readers}) exceeded")
        CheckStatusError.check(rc, "BroadcastRing.subscribe")
        sub = Subscriber(self, out_cur[0])
        self._subscribers.add(sub)
        return sub

    def close(self) -> None:
        if not self._destroyed and self._ptr != ffi.NULL:
            lib.ll_broadcast_close(self._ptr)
            drain_deferred_decrefs()

    def destroy(self) -> None:
        if not self._destroyed and self._ptr != ffi.NULL:
            for sub in list(self._subscribers):
                sub.unsubscribe()
            self._subscribers.clear()
            ptr = self._ptr
            self._ptr = ffi.NULL
            self._destroyed = True
            lib.ll_broadcast_destroy(ptr)
            drain_deferred_decrefs()

    def __enter__(self) -> BroadcastRing[T]:
        self._check_alive()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    def __del__(self) -> None:
        try:
            self.destroy()
        except Exception:
            pass


class ConcurrentDictSnapshot(Generic[K, V]):
    """O(1) linearizable read-only snapshot of a ``ConcurrentDict``."""

    __slots__ = ("_destroyed", "_ptr")

    def __init__(self, ptr: Any) -> None:
        self._ptr = ptr
        self._destroyed = False

    def _check_alive(self) -> None:
        if self._destroyed or self._ptr == ffi.NULL:
            raise Closed("ConcurrentDictSnapshot has been closed")

    def get(self, key: K, default: Any = None) -> Any:
        self._check_alive()
        kind, iv, fv, bv = encode_key(key)
        c_bytes = ffi.from_buffer("uint8_t[]", bv) if bv else ffi.NULL
        out_val = ffi.new("void**")
        enter_borrow()
        try:
            rc = lib.ll_ctrie_snapshot_get(self._ptr, kind, iv, fv, c_bytes, len(bv), out_val)
            if rc == LF_OK:
                return unbox_pyobj_borrow(out_val[0])
            if rc == LF_ERR_EMPTY:
                return default
            CheckStatusError.check(rc, "ConcurrentDictSnapshot.get")
            return default
        finally:
            exit_borrow()

    def __getitem__(self, key: K) -> V:
        val = self.get(key, _MISSING)
        if val is _MISSING:
            raise KeyError(key)
        return val

    def __contains__(self, key: object) -> bool:
        try:
            return self.get(key, _MISSING) is not _MISSING  # type: ignore[arg-type]
        except TypeError:
            return False

    def __len__(self) -> int:
        self._check_alive()
        return int(lib.ll_ctrie_snapshot_len(self._ptr))

    def _entries(self) -> list[tuple[K, V]]:
        self._check_alive()
        cap = max(len(self) + 16, 32)
        enter_borrow()
        try:
            while True:
                keys_buf = ffi.new("void*[]", cap)
                vals_buf = ffi.new("void*[]", cap)
                n = int(lib.ll_ctrie_snapshot_entries(self._ptr, keys_buf, vals_buf, cap))
                if n < cap:
                    break
                cap *= 2
            res: list[tuple[K, V]] = [
                (unbox_pyobj_borrow(keys_buf[i]), unbox_pyobj_borrow(vals_buf[i])) for i in range(n)
            ]
            return res
        finally:
            exit_borrow()

    def items(self) -> list[tuple[K, V]]:
        return self._entries()

    def keys(self) -> list[K]:
        return [k for k, _ in self._entries()]

    def values(self) -> list[V]:
        return [v for _, v in self._entries()]

    def to_dict(self) -> dict[K, V]:
        return dict(self._entries())

    def __iter__(self) -> Iterator[K]:
        return iter(self.keys())

    def close(self) -> None:
        if not self._destroyed and self._ptr != ffi.NULL:
            ptr = self._ptr
            self._ptr = ffi.NULL
            self._destroyed = True
            lib.ll_ctrie_snapshot_destroy(ptr)
            drain_deferred_decrefs()

    destroy = close

    def __enter__(self) -> ConcurrentDictSnapshot[K, V]:
        self._check_alive()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass


class ConcurrentDict(Generic[K, V]):
    """Lock-free concurrent hash array mapped trie (`Ctrie`) with O(1) snapshots (`HIGH-03`)."""

    __slots__ = ("_destroyed", "_ptr")

    def __init__(self, initial: dict[K, V] | None = None) -> None:
        out = ffi.new("ll_ctrie_t**")
        rc = lib.ll_ctrie_create(out)
        CheckStatusError.check(rc, "ConcurrentDict.__init__")
        self._ptr = out[0]
        self._destroyed = False
        if initial:
            for k, v in initial.items():
                self[k] = v

    def _check_alive(self) -> None:
        if self._destroyed or self._ptr == ffi.NULL:
            raise Closed("ConcurrentDict has been destroyed")

    def put(self, key: K, value: V) -> V | None:
        """Inserts or overwrites ``key`` with ``value``, returning the displaced value or ``None``."""
        self._check_alive()
        kind, iv, fv, bv = encode_key(key)
        c_bytes = ffi.from_buffer("uint8_t[]", bv) if bv else ffi.NULL
        k_ptr = box_pyobj(key)
        v_ptr = box_pyobj(value)
        out_replaced = ffi.new("bool*")
        out_old = ffi.new("void**")
        enter_borrow()
        try:
            rc = lib.ll_ctrie_put(
                self._ptr, kind, iv, fv, c_bytes, len(bv), k_ptr, v_ptr, out_replaced, out_old
            )
            if rc != LF_OK:
                decref_pyobj_ptr(k_ptr)
                decref_pyobj_ptr(v_ptr)
                CheckStatusError.check(rc, "ConcurrentDict.put")
            if bool(out_replaced[0]):
                return unbox_pyobj_borrow(out_old[0])
            return None
        finally:
            exit_borrow()

    def put_if_absent(self, key: K, value: V) -> V:
        """Atomically inserts ``value`` if ``key`` is absent, returning the current value."""
        self._check_alive()
        kind, iv, fv, bv = encode_key(key)
        c_bytes = ffi.from_buffer("uint8_t[]", bv) if bv else ffi.NULL
        k_ptr = box_pyobj(key)
        v_ptr = box_pyobj(value)
        out_inserted = ffi.new("bool*")
        out_existing = ffi.new("void**")
        enter_borrow()
        try:
            rc = lib.ll_ctrie_put_if_absent(
                self._ptr, kind, iv, fv, c_bytes, len(bv), k_ptr, v_ptr, out_inserted, out_existing
            )
            if rc != LF_OK:
                decref_pyobj_ptr(k_ptr)
                decref_pyobj_ptr(v_ptr)
                CheckStatusError.check(rc, "ConcurrentDict.put_if_absent")
            return unbox_pyobj_borrow(out_existing[0])
        finally:
            exit_borrow()

    def setdefault(self, key: K, default: V = None) -> V:  # type: ignore[assignment]
        return self.put_if_absent(key, default)

    def compute_if_absent(self, key: K, factory: Callable[[K], V]) -> V:
        """Returns existing value for ``key`` or computes and atomically inserts ``factory(key)``."""
        existing = self.get(key, _MISSING)
        if existing is not _MISSING:
            return existing
        computed = factory(key)
        return self.put_if_absent(key, computed)

    def get(self, key: K, default: Any = None) -> Any:
        self._check_alive()
        kind, iv, fv, bv = encode_key(key)
        c_bytes = ffi.from_buffer("uint8_t[]", bv) if bv else ffi.NULL
        out_val = ffi.new("void**")
        enter_borrow()
        try:
            rc = lib.ll_ctrie_get(self._ptr, kind, iv, fv, c_bytes, len(bv), out_val)
            if rc == LF_OK:
                return unbox_pyobj_borrow(out_val[0])
            if rc == LF_ERR_EMPTY:
                return default
            CheckStatusError.check(rc, "ConcurrentDict.get")
            return default
        finally:
            exit_borrow()

    def remove(self, key: K) -> V:
        """Removes ``key`` and returns its previous value, or raises ``KeyError``."""
        self._check_alive()
        kind, iv, fv, bv = encode_key(key)
        c_bytes = ffi.from_buffer("uint8_t[]", bv) if bv else ffi.NULL
        out_val = ffi.new("void**")
        enter_borrow()
        try:
            rc = lib.ll_ctrie_remove(self._ptr, kind, iv, fv, c_bytes, len(bv), out_val)
            if rc == LF_OK:
                return unbox_pyobj_borrow(out_val[0])
            if rc == LF_ERR_EMPTY:
                raise KeyError(key)
            CheckStatusError.check(rc, "ConcurrentDict.remove")
            raise KeyError(key)
        finally:
            exit_borrow()

    def pop(self, key: K, *default: Any) -> Any:
        if len(default) > 1:
            raise TypeError(f"pop expected at most 2 arguments, got {1 + len(default)}")
        try:
            return self.remove(key)
        except KeyError:
            if default:
                return default[0]
            raise

    def __getitem__(self, key: K) -> V:
        val = self.get(key, _MISSING)
        if val is _MISSING:
            raise KeyError(key)
        return val

    def __setitem__(self, key: K, value: V) -> None:
        self._check_alive()
        kind, iv, fv, bv = encode_key(key)
        c_bytes = ffi.from_buffer("uint8_t[]", bv) if bv else ffi.NULL
        k_ptr = box_pyobj(key)
        v_ptr = box_pyobj(value)
        rc = lib.ll_ctrie_put(
            self._ptr, kind, iv, fv, c_bytes, len(bv), k_ptr, v_ptr, ffi.NULL, ffi.NULL
        )
        if rc != LF_OK:
            decref_pyobj_ptr(k_ptr)
            decref_pyobj_ptr(v_ptr)
            CheckStatusError.check(rc, "ConcurrentDict.__setitem__")
        drain_deferred_decrefs()

    def __delitem__(self, key: K) -> None:
        self._check_alive()
        kind, iv, fv, bv = encode_key(key)
        c_bytes = ffi.from_buffer("uint8_t[]", bv) if bv else ffi.NULL
        rc = lib.ll_ctrie_remove(self._ptr, kind, iv, fv, c_bytes, len(bv), ffi.NULL)
        if rc == LF_OK:
            drain_deferred_decrefs()
            return
        if rc == LF_ERR_EMPTY:
            drain_deferred_decrefs()
            raise KeyError(key)
        CheckStatusError.check(rc, "ConcurrentDict.__delitem__")

    def __contains__(self, key: object) -> bool:
        try:
            return self.get(key, _MISSING) is not _MISSING  # type: ignore[arg-type]
        except TypeError:
            return False

    def __len__(self) -> int:
        self._check_alive()
        return int(lib.ll_ctrie_len(self._ptr))

    def _entries(self) -> list[tuple[K, V]]:
        self._check_alive()
        cap = max(len(self) + 16, 32)
        enter_borrow()
        try:
            while True:
                keys_buf = ffi.new("void*[]", cap)
                vals_buf = ffi.new("void*[]", cap)
                n = int(lib.ll_ctrie_entries(self._ptr, keys_buf, vals_buf, cap))
                if n < cap:
                    break
                cap *= 2
            res: list[tuple[K, V]] = [
                (unbox_pyobj_borrow(keys_buf[i]), unbox_pyobj_borrow(vals_buf[i])) for i in range(n)
            ]
            return res
        finally:
            exit_borrow()

    def items(self) -> list[tuple[K, V]]:
        return self._entries()

    def keys(self) -> list[K]:
        return [k for k, _ in self._entries()]

    def values(self) -> list[V]:
        return [v for _, v in self._entries()]

    def __iter__(self) -> Iterator[K]:
        return iter(self.keys())

    def clear(self) -> None:
        for k in self.keys():
            try:
                self.remove(k)
            except KeyError:
                pass
        drain_deferred_decrefs()

    def snapshot(self) -> ConcurrentDictSnapshot[K, V]:
        """Captures an O(1) linearizable point-in-time snapshot."""
        self._check_alive()
        out_snap = ffi.new("ll_ctrie_snapshot_t**")
        rc = lib.ll_ctrie_snapshot(self._ptr, out_snap)
        CheckStatusError.check(rc, "ConcurrentDict.snapshot")
        return ConcurrentDictSnapshot(out_snap[0])

    def destroy(self) -> None:
        if not self._destroyed and self._ptr != ffi.NULL:
            ptr = self._ptr
            self._ptr = ffi.NULL
            self._destroyed = True
            lib.ll_ctrie_destroy(ptr)
            drain_deferred_decrefs()

    close = destroy

    def __enter__(self) -> ConcurrentDict[K, V]:
        self._check_alive()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.destroy()

    def __del__(self) -> None:
        try:
            self.destroy()
        except Exception:
            pass


Ctrie = ConcurrentDict


class SkipListMap(Generic[K, V]):
    """Lock-free ordered key-value map (`SkipListMap`) maintaining keys in ascending sorted order."""

    __slots__ = ("_destroyed", "_ptr")

    def __init__(self, initial: dict[K, V] | None = None) -> None:
        out = ffi.new("ll_skiplist_t**")
        rc = lib.ll_skiplist_create(out)
        CheckStatusError.check(rc, "SkipListMap.__init__")
        self._ptr = out[0]
        self._destroyed = False
        if initial:
            for k, v in initial.items():
                self[k] = v

    def _check_alive(self) -> None:
        if self._destroyed or self._ptr == ffi.NULL:
            raise Closed("SkipListMap has been destroyed")

    def put(self, key: K, value: V) -> bool:
        """Inserts or updates ``key`` with ``value``, returning ``True`` if newly inserted."""
        self._check_alive()
        kind, iv, fv, bv = encode_key(key, ordered=True)
        c_bytes = ffi.from_buffer("uint8_t[]", bv) if bv else ffi.NULL
        k_ptr = box_pyobj(key)
        v_ptr = box_pyobj(value)
        out_inserted = ffi.new("bool*")
        rc = lib.ll_skiplist_put(
            self._ptr, kind, iv, fv, c_bytes, len(bv), k_ptr, v_ptr, out_inserted
        )
        if rc != LF_OK:
            decref_pyobj_ptr(k_ptr)
            decref_pyobj_ptr(v_ptr)
            CheckStatusError.check(rc, "SkipListMap.put")
        drain_deferred_decrefs()
        return bool(out_inserted[0])

    def put_if_absent(self, key: K, value: V) -> V:
        """Atomically inserts ``value`` if ``key`` is absent, returning the settled value."""
        self._check_alive()
        kind, iv, fv, bv = encode_key(key, ordered=True)
        c_bytes = ffi.from_buffer("uint8_t[]", bv) if bv else ffi.NULL
        k_ptr = box_pyobj(key)
        v_ptr = box_pyobj(value)
        out_val = ffi.new("void**")
        enter_borrow()
        try:
            rc = lib.ll_skiplist_put_if_absent(
                self._ptr, kind, iv, fv, c_bytes, len(bv), k_ptr, v_ptr, out_val
            )
            if rc != LF_OK:
                decref_pyobj_ptr(k_ptr)
                decref_pyobj_ptr(v_ptr)
                CheckStatusError.check(rc, "SkipListMap.put_if_absent")
            return unbox_pyobj_borrow(out_val[0])
        finally:
            exit_borrow()

    def setdefault(self, key: K, default: V = None) -> V:  # type: ignore[assignment]
        return self.put_if_absent(key, default)

    def compute_if_absent(self, key: K, factory: Callable[[K], V]) -> V:
        existing = self.get(key, _MISSING)
        if existing is not _MISSING:
            return existing
        computed = factory(key)
        return self.put_if_absent(key, computed)

    def get(self, key: K, default: Any = None) -> Any:
        self._check_alive()
        kind, iv, fv, bv = encode_key(key, ordered=True)
        c_bytes = ffi.from_buffer("uint8_t[]", bv) if bv else ffi.NULL
        out_val = ffi.new("void**")
        enter_borrow()
        try:
            rc = lib.ll_skiplist_get(self._ptr, kind, iv, fv, c_bytes, len(bv), out_val)
            if rc == LF_OK:
                return unbox_pyobj_borrow(out_val[0])
            if rc == LF_ERR_EMPTY:
                return default
            CheckStatusError.check(rc, "SkipListMap.get")
            return default
        finally:
            exit_borrow()

    def remove(self, key: K) -> V:
        """Removes ``key`` and returns its previous value, or raises ``KeyError``."""
        self._check_alive()
        kind, iv, fv, bv = encode_key(key, ordered=True)
        c_bytes = ffi.from_buffer("uint8_t[]", bv) if bv else ffi.NULL
        out_val = ffi.new("void**")
        enter_borrow()
        try:
            rc = lib.ll_skiplist_remove(self._ptr, kind, iv, fv, c_bytes, len(bv), out_val)
            if rc == LF_OK:
                return unbox_pyobj_borrow(out_val[0])
            if rc == LF_ERR_EMPTY:
                raise KeyError(key)
            CheckStatusError.check(rc, "SkipListMap.remove")
            raise KeyError(key)
        finally:
            exit_borrow()

    def pop(self, key: K, *default: Any) -> Any:
        if len(default) > 1:
            raise TypeError(f"pop expected at most 2 arguments, got {1 + len(default)}")
        try:
            return self.remove(key)
        except KeyError:
            if default:
                return default[0]
            raise

    def __getitem__(self, key: K) -> V:
        val = self.get(key, _MISSING)
        if val is _MISSING:
            raise KeyError(key)
        return val

    def __setitem__(self, key: K, value: V) -> None:
        self.put(key, value)

    def __delitem__(self, key: K) -> None:
        self.remove(key)

    def __contains__(self, key: object) -> bool:
        try:
            return self.get(key, _MISSING) is not _MISSING  # type: ignore[arg-type]
        except TypeError:
            return False

    def __len__(self) -> int:
        self._check_alive()
        return int(lib.ll_skiplist_len(self._ptr))

    def _entries(self) -> list[tuple[K, V]]:
        self._check_alive()
        cap = max(len(self) + 16, 32)
        enter_borrow()
        try:
            while True:
                keys_buf = ffi.new("void*[]", cap)
                vals_buf = ffi.new("void*[]", cap)
                n = int(lib.ll_skiplist_entries(self._ptr, keys_buf, vals_buf, cap))
                if n < cap:
                    break
                cap *= 2
            res: list[tuple[K, V]] = [
                (unbox_pyobj_borrow(keys_buf[i]), unbox_pyobj_borrow(vals_buf[i])) for i in range(n)
            ]
            return res
        finally:
            exit_borrow()

    def items(self) -> list[tuple[K, V]]:
        """Returns ``(key, value)`` pairs in strictly ascending key order."""
        return self._entries()

    def keys(self) -> list[K]:
        """Returns keys in strictly ascending key order."""
        return [k for k, _ in self._entries()]

    def values(self) -> list[V]:
        """Returns values ordered by their corresponding ascending keys."""
        return [v for _, v in self._entries()]

    def __iter__(self) -> Iterator[K]:
        return iter(self.keys())

    def clear(self) -> None:
        for k in self.keys():
            try:
                self.remove(k)
            except KeyError:
                pass
        drain_deferred_decrefs()

    def destroy(self) -> None:
        if not self._destroyed and self._ptr != ffi.NULL:
            ptr = self._ptr
            self._ptr = ffi.NULL
            self._destroyed = True
            lib.ll_skiplist_destroy(ptr)
            drain_deferred_decrefs()

    close = destroy

    def __enter__(self) -> SkipListMap[K, V]:
        self._check_alive()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.destroy()

    def __del__(self) -> None:
        try:
            self.destroy()
        except Exception:
            pass


class SkipListSet(Generic[T]):
    """Lock-free ordered set (`SkipListSet`) maintaining items in ascending sorted order."""

    __slots__ = ("_destroyed", "_ptr")

    def __init__(self, initial: Any = None) -> None:
        out = ffi.new("ll_skiplist_set_t**")
        rc = lib.ll_skiplist_set_create(out)
        CheckStatusError.check(rc, "SkipListSet.__init__")
        self._ptr = out[0]
        self._destroyed = False
        if initial is not None:
            for item in initial:
                self.add(item)

    def _check_alive(self) -> None:
        if self._destroyed or self._ptr == ffi.NULL:
            raise Closed("SkipListSet has been destroyed")

    def add(self, item: T) -> bool:
        """Adds ``item`` to the set, returning ``True`` if newly inserted or ``False`` if already present."""
        self._check_alive()
        kind, iv, fv, bv = encode_key(item, ordered=True)
        c_bytes = ffi.from_buffer("uint8_t[]", bv) if bv else ffi.NULL
        item_ptr = box_pyobj(item)
        out_added = ffi.new("bool*")
        rc = lib.ll_skiplist_set_add(self._ptr, kind, iv, fv, c_bytes, len(bv), item_ptr, out_added)
        if rc != LF_OK:
            decref_pyobj_ptr(item_ptr)
            CheckStatusError.check(rc, "SkipListSet.add")
        drain_deferred_decrefs()
        return bool(out_added[0])

    def remove(self, item: T) -> None:
        """Removes ``item`` from the set, raising ``KeyError`` if absent."""
        self._check_alive()
        kind, iv, fv, bv = encode_key(item, ordered=True)
        c_bytes = ffi.from_buffer("uint8_t[]", bv) if bv else ffi.NULL
        rc = lib.ll_skiplist_set_remove(self._ptr, kind, iv, fv, c_bytes, len(bv))
        if rc == LF_OK:
            drain_deferred_decrefs()
            return
        if rc == LF_ERR_EMPTY:
            drain_deferred_decrefs()
            raise KeyError(item)
        CheckStatusError.check(rc, "SkipListSet.remove")

    def discard(self, item: T) -> bool:
        """Removes ``item`` if present, returning ``True`` if removed or ``False`` if absent."""
        try:
            self.remove(item)
            return True
        except KeyError:
            return False

    def __contains__(self, item: object) -> bool:
        self._check_alive()
        try:
            kind, iv, fv, bv = encode_key(item, ordered=True)
        except TypeError:
            return False
        c_bytes = ffi.from_buffer("uint8_t[]", bv) if bv else ffi.NULL
        res = bool(lib.ll_skiplist_set_contains(self._ptr, kind, iv, fv, c_bytes, len(bv)))
        drain_deferred_decrefs()
        return res

    def __len__(self) -> int:
        self._check_alive()
        return int(lib.ll_skiplist_set_len(self._ptr))

    def to_list(self) -> list[T]:
        """Returns all elements in strictly ascending sorted order."""
        self._check_alive()
        cap = max(len(self) + 16, 32)
        enter_borrow()
        try:
            while True:
                items_buf = ffi.new("void*[]", cap)
                n = int(lib.ll_skiplist_set_items(self._ptr, items_buf, cap))
                if n < cap:
                    break
                cap *= 2
            res: list[T] = [unbox_pyobj_borrow(items_buf[i]) for i in range(n)]
            return res
        finally:
            exit_borrow()

    def __iter__(self) -> Iterator[T]:
        return iter(self.to_list())

    def clear(self) -> None:
        for item in self.to_list():
            self.discard(item)
        drain_deferred_decrefs()

    def destroy(self) -> None:
        if not self._destroyed and self._ptr != ffi.NULL:
            ptr = self._ptr
            self._ptr = ffi.NULL
            self._destroyed = True
            lib.ll_skiplist_set_destroy(ptr)
            drain_deferred_decrefs()

    close = destroy

    def __enter__(self) -> SkipListSet[T]:
        self._check_alive()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.destroy()

    def __del__(self) -> None:
        try:
            self.destroy()
        except Exception:
            pass
