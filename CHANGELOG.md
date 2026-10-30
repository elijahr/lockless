# Changelog

All notable changes to `lockless` are documented in this file.

## [Unreleased]

- Updated `lockless.nimble` dependencies to `requires "lockfree >= 0.6.6 & < 0.7.0"` and `requires "typestates >= 0.14.0 & < 0.15.0"`.
- Security & concurrency hardening across `liblockless_ext` and Python bindings:
  - Removed all `pickle` usage (`import pickle`, `pickle.dumps`) from `lockless._cabi`; implemented zero-pickle order-preserving byte encoding for arbitrary-precision big integers, IEEE-754 floats, strings, bytes, and nested scalar tuples in `ConcurrentDict`, `SkipListMap`, and `SkipListSet`.
  - Added cross-thread atomic borrower tracking (`ll_enter_borrow` / `ll_exit_borrow`) so `drain_deferred_decrefs()` never reclaims retired `PyObject*` pointers while any thread holds a borrowed pointer.
  - Guarded `float64`-to-`int64` conversions in `LlKey` equality and hashing against `NaN`, `Inf`, `-Inf`, and out-of-range floats, and retained `PyKVBox` ownership across `reclaimNow` in `ll_skiplist_remove`.
  - Isolated per-thread default producer/consumer handles via `threading.local()` on unbounded `MpmcQueue` and tracked open handles/subscribers via `weakref.WeakSet` on `MpmcQueue` and `BroadcastRing`.
  - Switched `Channel` event-loop reader callbacks to weak references, removed readers on `close()`, and avoided wakeup pipe writes when no async readers are registered.
  - Aligned rate-limiter C declarations in `include/lockless_ext.h` and validated non-negative integer `start` bounds in `TaskPool.parallel_for`.
- Initial release of `lockless` standalone Python concurrency & DSP package backed by `lockfree` Nim C-ABI (`liblockless_ext`):
  - Wait-free and lock-free queues: `BQueue128`, `SpscQueue`, and `MpmcQueue` with `PyObject*` reference transfer and batch operations.
  - Zero-copy SPSC DSP ring buffer: `StreamBuffer` with `reserve_write(n)` and `peek_read(max_n)` context managers returning writable/read-only `memoryview` pairs `(first_view, second_view)`.
  - Wait-free pub/sub ring: `BroadcastRing` and `Subscriber` cursors with epoch-safe reference counting on `omDropOldest` eviction and destruction.
  - Lock-free associative containers: `ConcurrentDict` (`Ctrie`), `SkipListMap`, and `SkipListSet` with atomic `put`, `get`, `remove`, `pop`, `compute_if_absent`, `O(1)` `snapshot()`, and safe displaced-item `Py_DecRef`.
  - Synchronous & `asyncio` channels: `Channel` and `RendezvousChannel` with 0-to-1 wakeup integration (`lf_bqueue128_push_ex` + OS pipe reader with Dekker fence) and `async for item in ch:` support.
  - Work-stealing native thread pool (`TaskPool`) and lock-free 128-bit CAS rate limiters (`TokenBucket`, `LeakyBucket`).
  - Complete `.pyi` type stubs (`python/lockless/__init__.pyi`) and `py.typed` marker.
