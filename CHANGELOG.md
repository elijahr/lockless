# Changelog

All notable changes to `lockless` are documented in this file.

## [Unreleased]

- Initial release of `lockless` standalone Python concurrency & DSP package backed by `lockfree` Nim C-ABI (`liblockless_ext`):
  - Wait-free and lock-free queues: `BQueue128`, `SpscQueue`, and `MpmcQueue` with `PyObject*` reference transfer and batch operations.
  - Zero-copy SPSC DSP ring buffer: `StreamBuffer` with `reserve_write(n)` and `peek_read(max_n)` context managers returning writable/read-only `memoryview` pairs `(first_view, second_view)`.
  - Wait-free pub/sub ring: `BroadcastRing` and `Subscriber` cursors with epoch-safe reference counting on `omDropOldest` eviction and destruction.
  - Lock-free associative containers: `ConcurrentDict` (`Ctrie`), `SkipListMap`, and `SkipListSet` with atomic `put`, `get`, `remove`, `pop`, `compute_if_absent`, `O(1)` `snapshot()`, and safe displaced-item `Py_DecRef`.
  - Synchronous & `asyncio` channels: `Channel` and `RendezvousChannel` with 0-to-1 wakeup integration (`lf_bqueue128_push_ex` + OS pipe reader with Dekker fence) and `async for item in ch:` support.
  - Work-stealing native thread pool (`TaskPool`) and lock-free 128-bit CAS rate limiters (`TokenBucket`, `LeakyBucket`).
  - Complete `.pyi` type stubs (`python/lockless/__init__.pyi`) and `py.typed` marker.
