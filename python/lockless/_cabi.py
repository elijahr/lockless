"""C-ABI bindings, status codes, exceptions, and PyObject* reference helpers for lockless."""

from __future__ import annotations

import ctypes
import ctypes.util
import pickle
import sys
from pathlib import Path
from typing import Any

from cffi import FFI

# ==============================================================================
# Exception Hierarchy
# ==============================================================================


class LocklessError(Exception):
    """Base exception for all lockless errors."""


class Empty(LocklessError):
    """Raised when a non-blocking pop/recv/poll encounters an empty container."""


class Full(LocklessError):
    """Raised when a non-blocking push/send/publish encounters a full container."""


class Closed(LocklessError):
    """Raised when an operation is attempted on a closed queue, channel, or ring."""


# ==============================================================================
# C-ABI Status Codes & Constants
# ==============================================================================

LF_ERR_PANIC = -2
LF_ERR_FAILURE = -1
LF_OK = 0
LF_ERR_EMPTY = 1
LF_ERR_FULL = 2
LF_ERR_CLOSED = 3
LF_ERR_INVALID_ARG = 4
LF_ERR_REGISTRY_FULL = 5
LF_ERR_UNSUPPORTED = 6
LF_ERR_TIMEOUT = 7

LF_OVERFLOW_DROP_OLDEST = 0
LF_OVERFLOW_BACKOFF = 1
LF_OVERFLOW_BACKPRESSURE = 1

LF_SUB_FROM_LATEST = 0
LF_SUB_FROM_EARLIEST = 1

LF_POLL_SUCCESS = 0
LF_POLL_EMPTY = 1
LF_POLL_LAGGED = 2


def check_status(status: int, msg: str = "lockless C-ABI error") -> None:
    """Translates a non-zero C-ABI status code into the corresponding Python exception."""
    if status == LF_OK:
        return
    if status == LF_ERR_EMPTY:
        raise Empty("Container is empty")
    if status == LF_ERR_FULL:
        raise Full("Container is full")
    if status == LF_ERR_CLOSED:
        raise Closed("Container is closed")
    if status == LF_ERR_INVALID_ARG:
        raise ValueError(f"{msg}: invalid argument (LF_ERR_INVALID_ARG)")
    if status == LF_ERR_REGISTRY_FULL:
        raise RuntimeError(f"{msg}: thread/cursor registry full (LF_ERR_REGISTRY_FULL)")
    if status == LF_ERR_UNSUPPORTED:
        raise ValueError(f"{msg}: unsupported capacity or configuration (LF_ERR_UNSUPPORTED)")
    if status == LF_ERR_TIMEOUT:
        raise TimeoutError(f"{msg}: operation timed out")
    raise LocklessError(f"{msg} (status={status})")


class CheckStatusError:
    check = staticmethod(check_status)


# ==============================================================================
# CFFI Declarations & Shared Library Loading
# ==============================================================================

ffi = FFI()
ffi.cdef(
    """
    typedef enum lf_status_t {
        LF_ERR_PANIC         = -2,
        LF_ERR_FAILURE       = -1,
        LF_OK                =  0,
        LF_ERR_EMPTY         =  1,
        LF_ERR_FULL          =  2,
        LF_ERR_CLOSED        =  3,
        LF_ERR_INVALID_ARG   =  4,
        LF_ERR_REGISTRY_FULL =  5,
        LF_ERR_UNSUPPORTED   =  6
    } lf_status_t;

    typedef enum lf_overflow_mode_t {
        LF_OVERFLOW_DROP_OLDEST = 0,
        LF_OVERFLOW_BACKOFF     = 1
    } lf_overflow_mode_t;

    typedef enum lf_sub_origin_t {
        LF_SUB_FROM_LATEST   = 0,
        LF_SUB_FROM_EARLIEST = 1
    } lf_sub_origin_t;

    typedef enum lf_poll_result_t {
        LF_POLL_SUCCESS = 0,
        LF_POLL_EMPTY   = 1,
        LF_POLL_LAGGED  = 2
    } lf_poll_result_t;

    typedef struct lf_queue_t lf_queue_t;
    typedef struct lf_producer_t lf_producer_t;
    typedef struct lf_consumer_t lf_consumer_t;
    typedef struct lf_bqueue128_t lf_bqueue128_t;
    typedef struct ll_spsc_t ll_spsc_t;
    typedef struct lf_stream_ring_t lf_streambuffer_t;
    typedef struct ll_broadcast_t ll_broadcast_t;
    typedef struct ll_broadcast_cursor_t ll_broadcast_cursor_t;
    typedef struct ll_ctrie_t ll_ctrie_t;
    typedef struct ll_ctrie_snapshot_t ll_ctrie_snapshot_t;
    typedef struct ll_skiplist_t ll_skiplist_t;
    typedef struct ll_skiplist_set_t ll_skiplist_set_t;
    typedef struct lf_rendezvous_t lf_rendezvous_t;
    typedef struct lf_taskpool_t lf_taskpool_t;
    typedef struct lf_token_bucket_t lf_token_bucket_t;
    typedef struct lf_leaky_bucket_t lf_leaky_bucket_t;

    typedef void (*lf_item_destructor_fn)(void* item, void* user_data);
    typedef void (*lf_bqueue128_destructor_fn)(void* item, void* user_data);
    typedef void (*lf_task_fn)(void* arg);
    typedef void (*lf_for_task_fn)(size_t index, void* arg);

    /* Deferred Py_DecRef queue */
    void ll_retire_pyobj(void* pyobj);
    bool ll_pop_deferred_decref(void** out_pyobj);
    size_t ll_pop_deferred_decref_batch(void** out_pyobjs, size_t max_count);
    size_t ll_deferred_decref_count(void);

    /* BQueue128 */
    lf_status_t lf_bqueue128_create(size_t capacity, lf_bqueue128_destructor_fn destructor, void* user_data, lf_bqueue128_t** out_queue);
    lf_status_t lf_bqueue128_destroy(lf_bqueue128_t* queue);
    lf_status_t lf_bqueue128_push(lf_bqueue128_t* queue, void* item);
    lf_status_t lf_bqueue128_push_ex(lf_bqueue128_t* queue, void* item, bool* out_was_empty);
    lf_status_t lf_bqueue128_pop(lf_bqueue128_t* queue, void** out_item);
    lf_status_t lf_bqueue128_peek(lf_bqueue128_t* queue, void** out_item);
    lf_status_t lf_bqueue128_clear(lf_bqueue128_t* queue);
    size_t lf_bqueue128_len(lf_bqueue128_t* queue);
    size_t lf_bqueue128_capacity(lf_bqueue128_t* queue);
    bool lf_bqueue128_is_empty(lf_bqueue128_t* queue);
    bool lf_bqueue128_is_full(lf_bqueue128_t* queue);

    /* SpscQueue */
    lf_status_t ll_spsc_create(size_t capacity, ll_spsc_t** out_queue);
    lf_status_t ll_spsc_destroy(ll_spsc_t* q);
    lf_status_t ll_spsc_close(ll_spsc_t* q);
    bool ll_spsc_is_closed(ll_spsc_t* q);
    lf_status_t ll_spsc_push(ll_spsc_t* q, void* item);
    lf_status_t ll_spsc_push_ex(ll_spsc_t* q, void* item, bool* out_was_empty);
    lf_status_t ll_spsc_pop(ll_spsc_t* q, void** out_item);
    lf_status_t ll_spsc_peek(ll_spsc_t* q, void** out_item);
    size_t ll_spsc_pop_batch(ll_spsc_t* q, void** out_items, size_t max_count);
    lf_status_t ll_spsc_clear(ll_spsc_t* q);
    size_t ll_spsc_len(ll_spsc_t* q);
    size_t ll_spsc_capacity(ll_spsc_t* q);
    bool ll_spsc_is_empty(ll_spsc_t* q);
    bool ll_spsc_is_full(ll_spsc_t* q);

    /* MpmcQueue */
    lf_status_t lf_bounded_mpmc_create(size_t capacity, size_t max_producers, size_t max_consumers, lf_item_destructor_fn destructor, void* user_data, lf_queue_t** out_queue);
    lf_status_t lf_unbounded_mpmc_create(size_t segment_size, size_t max_threads, lf_item_destructor_fn destructor, void* user_data, lf_queue_t** out_queue);
    lf_status_t lf_queue_close(lf_queue_t* queue);
    lf_status_t lf_queue_destroy(lf_queue_t* queue);
    lf_status_t lf_producer_acquire(lf_queue_t* queue, lf_producer_t** out_prod);
    lf_status_t lf_producer_release(lf_producer_t* prod);
    lf_status_t lf_consumer_acquire(lf_queue_t* queue, lf_consumer_t** out_cons);
    lf_status_t lf_consumer_release(lf_consumer_t* cons);
    lf_status_t lf_push(lf_producer_t* prod, void* item);
    lf_status_t lf_push_ex(lf_producer_t* prod, void* item, bool* out_was_empty);
    lf_status_t lf_pop(lf_consumer_t* cons, void** out_item);
    size_t lf_pop_batch(lf_consumer_t* cons, void** out_items, size_t max_count);
    size_t lf_queue_len(lf_queue_t* queue);
    bool lf_queue_is_empty(lf_queue_t* queue);
    bool lf_queue_is_closed(lf_queue_t* queue);

    /* StreamBuffer */
    lf_streambuffer_t* lf_streambuffer_create(size_t capacity, bool use_virtual_mirror);
    void lf_streambuffer_destroy(lf_streambuffer_t* ring);
    size_t lf_streambuffer_capacity(lf_streambuffer_t* ring);
    size_t lf_streambuffer_available_read(lf_streambuffer_t* ring);
    size_t lf_streambuffer_available_write(lf_streambuffer_t* ring);
    bool lf_streambuffer_is_empty(lf_streambuffer_t* ring);
    bool lf_streambuffer_is_full(lf_streambuffer_t* ring);
    size_t lf_streambuffer_try_write(lf_streambuffer_t* ring, const void* src, size_t len);
    size_t lf_streambuffer_try_read(lf_streambuffer_t* ring, void* dst, size_t max_len);
    lf_status_t lf_stream_reserve_write(lf_streambuffer_t* sb, size_t requested_bytes, uint8_t** out_ptr1, size_t* out_len1, uint8_t** out_ptr2, size_t* out_len2);
    lf_status_t lf_stream_commit_write(lf_streambuffer_t* sb, size_t committed_bytes);
    lf_status_t lf_stream_peek_read(lf_streambuffer_t* sb, uint8_t** out_ptr1, size_t* out_len1, uint8_t** out_ptr2, size_t* out_len2);
    lf_status_t lf_stream_consume_read(lf_streambuffer_t* sb, size_t consumed_bytes);

    /* BroadcastRing */
    lf_status_t ll_broadcast_create(size_t capacity, lf_overflow_mode_t overflow_mode, size_t max_readers, ll_broadcast_t** out_broadcast);
    lf_status_t ll_broadcast_close(ll_broadcast_t* broadcast);
    bool ll_broadcast_is_closed(ll_broadcast_t* broadcast);
    lf_status_t ll_broadcast_destroy(ll_broadcast_t* broadcast);
    lf_status_t ll_broadcast_publish(ll_broadcast_t* broadcast, void* item);
    lf_status_t ll_broadcast_try_publish(ll_broadcast_t* broadcast, void* item);
    lf_status_t ll_broadcast_subscribe(ll_broadcast_t* broadcast, lf_sub_origin_t origin, ll_broadcast_cursor_t** out_cursor);
    lf_status_t ll_broadcast_unsubscribe(ll_broadcast_cursor_t* cursor);
    lf_status_t ll_broadcast_poll(ll_broadcast_cursor_t* cursor, void** out_item, size_t* out_skipped_count, lf_poll_result_t* out_result);
    size_t ll_broadcast_cursor_lag(ll_broadcast_cursor_t* cursor);
    size_t ll_broadcast_len(ll_broadcast_t* broadcast);
    size_t ll_broadcast_capacity(ll_broadcast_t* broadcast);
    size_t ll_broadcast_subscriber_count(ll_broadcast_t* broadcast);

    /* ConcurrentDict (Ctrie) */
    lf_status_t ll_ctrie_create(ll_ctrie_t** out_ctrie);
    lf_status_t ll_ctrie_destroy(ll_ctrie_t* ctrie);
    lf_status_t ll_ctrie_put(ll_ctrie_t* ctrie, uint8_t key_kind, int64_t key_int, double key_float, const void* key_bytes, size_t key_len, void* key_pyobj, void* val_pyobj, bool* out_replaced, void** out_old_val_pyobj);
    lf_status_t ll_ctrie_put_if_absent(ll_ctrie_t* ctrie, uint8_t key_kind, int64_t key_int, double key_float, const void* key_bytes, size_t key_len, void* key_pyobj, void* val_pyobj, bool* out_inserted, void** out_existing_val_pyobj);
    lf_status_t ll_ctrie_get(ll_ctrie_t* ctrie, uint8_t key_kind, int64_t key_int, double key_float, const void* key_bytes, size_t key_len, void** out_val_pyobj);
    lf_status_t ll_ctrie_remove(ll_ctrie_t* ctrie, uint8_t key_kind, int64_t key_int, double key_float, const void* key_bytes, size_t key_len, void** out_val_pyobj);
    size_t ll_ctrie_len(ll_ctrie_t* ctrie);
    size_t ll_ctrie_entries(ll_ctrie_t* ctrie, void** out_keys, void** out_vals, size_t max_count);
    lf_status_t ll_ctrie_snapshot(ll_ctrie_t* ctrie, ll_ctrie_snapshot_t** out_snap);
    lf_status_t ll_ctrie_snapshot_destroy(ll_ctrie_snapshot_t* snap);
    lf_status_t ll_ctrie_snapshot_get(ll_ctrie_snapshot_t* snap, uint8_t key_kind, int64_t key_int, double key_float, const void* key_bytes, size_t key_len, void** out_val_pyobj);
    size_t ll_ctrie_snapshot_len(ll_ctrie_snapshot_t* snap);
    size_t ll_ctrie_snapshot_entries(ll_ctrie_snapshot_t* snap, void** out_keys, void** out_vals, size_t max_count);

    /* SkipListMap */
    lf_status_t ll_skiplist_create(ll_skiplist_t** out_map);
    lf_status_t ll_skiplist_destroy(ll_skiplist_t* map);
    lf_status_t ll_skiplist_put(ll_skiplist_t* map, uint8_t key_kind, int64_t key_int, double key_float, const void* key_bytes, size_t key_len, void* key_pyobj, void* val_pyobj, bool* out_inserted);
    lf_status_t ll_skiplist_put_if_absent(ll_skiplist_t* map, uint8_t key_kind, int64_t key_int, double key_float, const void* key_bytes, size_t key_len, void* key_pyobj, void* val_pyobj, void** out_val_pyobj);
    lf_status_t ll_skiplist_get(ll_skiplist_t* map, uint8_t key_kind, int64_t key_int, double key_float, const void* key_bytes, size_t key_len, void** out_val_pyobj);
    lf_status_t ll_skiplist_remove(ll_skiplist_t* map, uint8_t key_kind, int64_t key_int, double key_float, const void* key_bytes, size_t key_len, void** out_val_pyobj);
    size_t ll_skiplist_len(ll_skiplist_t* map);
    size_t ll_skiplist_entries(ll_skiplist_t* map, void** out_keys, void** out_vals, size_t max_count);

    /* SkipListSet */
    lf_status_t ll_skiplist_set_create(ll_skiplist_set_t** out_set);
    lf_status_t ll_skiplist_set_destroy(ll_skiplist_set_t* s);
    lf_status_t ll_skiplist_set_add(ll_skiplist_set_t* s, uint8_t key_kind, int64_t key_int, double key_float, const void* key_bytes, size_t key_len, void* item_pyobj, bool* out_added);
    bool ll_skiplist_set_contains(ll_skiplist_set_t* s, uint8_t key_kind, int64_t key_int, double key_float, const void* key_bytes, size_t key_len);
    lf_status_t ll_skiplist_set_remove(ll_skiplist_set_t* s, uint8_t key_kind, int64_t key_int, double key_float, const void* key_bytes, size_t key_len);
    size_t ll_skiplist_set_len(ll_skiplist_set_t* s);
    size_t ll_skiplist_set_items(ll_skiplist_set_t* s, void** out_items, size_t max_count);

    /* RendezvousChannel */
    lf_status_t lf_rendezvous_create(lf_rendezvous_t** out_chan);
    lf_status_t lf_rendezvous_destroy(lf_rendezvous_t* chan);
    lf_status_t lf_rendezvous_close(lf_rendezvous_t* chan);
    bool lf_rendezvous_is_closed(lf_rendezvous_t* chan);
    lf_status_t lf_rendezvous_send(lf_rendezvous_t* chan, void* payload, uint64_t* out_corr_id);
    lf_status_t lf_rendezvous_recv(lf_rendezvous_t* chan, void** out_payload, uint64_t* out_corr_id);
    lf_status_t lf_rendezvous_try_send(lf_rendezvous_t* chan, void* payload, uint64_t* out_corr_id);
    lf_status_t lf_rendezvous_try_recv(lf_rendezvous_t* chan, void** out_payload, uint64_t* out_corr_id);
    lf_status_t lf_rendezvous_send_timeout(lf_rendezvous_t* chan, void* payload, int32_t timeout_ms, uint64_t* out_corr_id);
    lf_status_t lf_rendezvous_recv_timeout(lf_rendezvous_t* chan, void** out_payload, int32_t timeout_ms, uint64_t* out_corr_id);

    /* TaskPool */
    lf_status_t lf_taskpool_create(size_t num_threads, lf_taskpool_t** out_pool);
    lf_status_t lf_taskpool_destroy(lf_taskpool_t* pool);
    lf_status_t lf_taskpool_spawn(lf_taskpool_t* pool, lf_task_fn task, void* arg);
    lf_status_t lf_taskpool_parallel_for(lf_taskpool_t* pool, size_t start, size_t stop, lf_for_task_fn task, void* arg, size_t chunk_size);
    lf_status_t lf_taskpool_sync(lf_taskpool_t* pool);
    size_t lf_taskpool_num_workers(lf_taskpool_t* pool);

    /* Rate Limiters */
    lf_token_bucket_t* lf_token_bucket_create(uint64_t capacity, uint64_t refill_rate);
    void lf_token_bucket_destroy(lf_token_bucket_t* bucket);
    void lf_token_bucket_reset(lf_token_bucket_t* bucket, uint64_t tokens);
    bool lf_token_bucket_try_acquire(lf_token_bucket_t* bucket, uint64_t tokens);
    bool lf_token_bucket_acquire_timeout(lf_token_bucket_t* bucket, uint64_t tokens, int64_t max_wait_ns);
    uint64_t lf_token_bucket_available(lf_token_bucket_t* bucket);
    uint64_t lf_token_bucket_capacity(const lf_token_bucket_t* bucket);
    uint64_t lf_token_bucket_refill_rate(const lf_token_bucket_t* bucket);

    lf_leaky_bucket_t* lf_leaky_bucket_create(uint64_t burst_tolerance_ns, uint64_t leak_rate);
    void lf_leaky_bucket_destroy(lf_leaky_bucket_t* bucket);
    void lf_leaky_bucket_reset(lf_leaky_bucket_t* bucket);
    bool lf_leaky_bucket_try_consume(lf_leaky_bucket_t* bucket, uint64_t weight);
    bool lf_leaky_bucket_consume_timeout(lf_leaky_bucket_t* bucket, uint64_t weight, int64_t max_wait_ns);
    uint64_t lf_leaky_bucket_water_level(lf_leaky_bucket_t* bucket);
    uint64_t lf_leaky_bucket_burst_tolerance_ns(const lf_leaky_bucket_t* bucket);
    uint64_t lf_leaky_bucket_leak_rate(const lf_leaky_bucket_t* bucket);
    """
)


def _load_library() -> Any:
    import subprocess

    pkg_dir = Path(__file__).resolve().parent
    candidates = [
        pkg_dir / "liblockless_ext.dylib",
        pkg_dir / "liblockless_ext.so",
        pkg_dir / "lockless_ext.dll",
    ]
    for path in candidates:
        if path.exists():
            return ffi.dlopen(str(path))

    repo_root = pkg_dir.parent.parent
    nim_src = repo_root / "src" / "lockless_ext.nim"
    if nim_src.exists():
        ext = (
            ".dylib" if sys.platform == "darwin" else (".dll" if sys.platform == "win32" else ".so")
        )
        out_path = pkg_dir / f"liblockless_ext{ext}"
        lockfree_src = repo_root.parent / "lockfree" / "src"
        if not lockfree_src.exists():
            lockfree_src = Path("/Users/eek/Development/lockfree/src")
        cmd = [
            "nim",
            "c",
            "--app:lib",
            "--noMain",
            "--mm:orc",
            "--threads:on",
            "-d:useMalloc",
            "-d:noSignalHandler",
            "-d:release",
            f"--path:{lockfree_src}",
            f"-o:{out_path}",
            str(nim_src),
        ]
        subprocess.run(cmd, check=True, cwd=str(repo_root))
        if out_path.exists():
            return ffi.dlopen(str(out_path))

    raise RuntimeError(f"Failed to locate compiled liblockless_ext shared library in {pkg_dir}")


lib = _load_library()

# ==============================================================================
# CPython PyObject* Reference Accounting & Wait-Free Deferred DecRef Drain
# ==============================================================================

_Py_IncRef = ctypes.pythonapi.Py_IncRef
_Py_IncRef.argtypes = [ctypes.py_object]
_Py_IncRef.restype = None

_Py_DecRef_Ptr = ctypes.pythonapi.Py_DecRef
_Py_DecRef_Ptr.argtypes = [ctypes.c_void_p]
_Py_DecRef_Ptr.restype = None

# Sentinel wrapper for `None` or `0` (NULL pointer) so raw `void*` slots can
# distinguish an empty NULL slot from a Python `None` payload.
_NONE_SENTINEL = object()

_DRAIN_BATCH_SIZE = 256
_active_borrowers = 0


def enter_borrow() -> None:
    """Marks the start of a C-ABI call that returns a borrowed `PyObject*`."""
    global _active_borrowers
    _active_borrowers += 1


def exit_borrow() -> None:
    """Marks the completion of unboxing a borrowed `PyObject*` under the GIL."""
    global _active_borrowers
    _active_borrowers -= 1
    if _active_borrowers == 0:
        drain_deferred_decrefs()


def drain_deferred_decrefs() -> int:
    """Drains all pending retired `PyObject*` pointers and calls `Py_DecRef` under the GIL."""
    if _active_borrowers > 0 or lib.ll_deferred_decref_count() == 0:
        return 0
    total = 0
    buf = ffi.new("void*[]", _DRAIN_BATCH_SIZE)
    while True:
        n = int(lib.ll_pop_deferred_decref_batch(buf, _DRAIN_BATCH_SIZE))
        if n == 0:
            break
        for i in range(n):
            addr = int(ffi.cast("uintptr_t", buf[i]))
            if addr != 0:
                _Py_DecRef_Ptr(addr)
        total += n
        if n < _DRAIN_BATCH_SIZE:
            break
    return total


def box_pyobj(obj: Any) -> Any:
    """Increments refcount of `obj` (or sentinel if None) and returns its `void*` CData pointer."""
    drain_deferred_decrefs()
    target = _NONE_SENTINEL if obj is None else obj
    _Py_IncRef(target)
    return ffi.cast("void*", id(target))


def unbox_pyobj_steal(ptr_cdata: Any) -> Any:
    """Transfers ownership from a container `void*` (`+1` ref) to a Python return value."""
    addr = int(ffi.cast("uintptr_t", ptr_cdata))
    if addr == 0:
        drain_deferred_decrefs()
        return None
    obj = ctypes.cast(addr, ctypes.py_object).value
    _Py_DecRef_Ptr(addr)
    drain_deferred_decrefs()
    if obj is _NONE_SENTINEL:
        return None
    return obj


def unbox_pyobj_borrow(ptr_cdata: Any) -> Any:
    """Reads a container `void*` without consuming the container's owned reference."""
    addr = int(ffi.cast("uintptr_t", ptr_cdata))
    if addr == 0:
        drain_deferred_decrefs()
        return None
    obj = ctypes.cast(addr, ctypes.py_object).value
    drain_deferred_decrefs()
    if obj is _NONE_SENTINEL:
        return None
    return obj


def decref_pyobj_ptr(ptr_cdata: Any) -> None:
    """Decrements the reference count of a `void*` `PyObject*` pointer if non-null."""
    addr = int(ffi.cast("uintptr_t", ptr_cdata))
    if addr != 0:
        _Py_DecRef_Ptr(addr)
    drain_deferred_decrefs()


# C-ABI callback passed to `lf_bqueue128_create` and `lf_bounded_mpmc_create` /
# `lf_unbounded_mpmc_create` so `clear()` and `destroy()` automatically enqueue
# remaining `PyObject*` pointers into the wait-free deferred decref queue.
@ffi.callback("void(void*, void*)")
def _pyobj_destructor_cb(item: Any, _user_data: Any) -> None:
    if item != ffi.NULL:
        lib.ll_retire_pyobj(item)


# ==============================================================================
# Key Encoding Helper for ConcurrentDict, SkipListMap, and SkipListSet
# ==============================================================================


def encode_key(key: Any) -> tuple[int, int, float, bytes]:
    """Encodes a hashable/comparable Python key into `(kind, int_val, float_val, bytes_val)`."""
    # Checkhashable first so unhashable types (e.g. list, dict) raise TypeError immediately
    hash(key)
    if isinstance(key, bool):
        return (0, 1 if key else 0, 0.0, b"")
    if isinstance(key, int):
        if -9223372036854775808 <= key <= 9223372036854775807:
            return (0, key, 0.0, b"")
        return (4, 0, 0.0, str(key).encode("utf-8"))
    if isinstance(key, float):
        return (1, 0, key, b"")
    if isinstance(key, str):
        return (2, 0, 0.0, key.encode("utf-8"))
    if isinstance(key, bytes):
        return (3, 0, 0.0, key)
    return (4, 0, 0.0, pickle.dumps(key, protocol=5))
