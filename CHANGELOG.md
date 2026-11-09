# Changelog

All notable changes to `lockless` are documented in this file.

## [Unreleased]

- Migrated `python/lockless/_codegen.py` (`parse_lockless_header`) to `headerkit`'s first-class `NimBackend` (`backend_name="nim"` and `clean_nim_c_unit`), removing references to the deleted `NimCleanupTransform` and `NimJsonDocEnricher` classes.
- Refactored `python/lockless/_codegen.py` to delegate C header parsing and `NimCleanupTransform` / `NimJsonDocEnricher` transformations directly to `headerkit.generate()`, removing manual `backend.parse()`, the custom `extract_nim_jsondoc()` subprocess wrapper, and sibling relative `sys.path` / `pythonpath` overrides.
- Replaced non-atomic `get(k)` + `put(k)` / `delete(k)` pre-read sequences in `ll_skiplist_put` and `ll_skiplist_remove` (`src/lockless_ext.nim`) with atomic `SkipListMap.putPrev` and `SkipListMap.deletePrev`, removed redundant double `generate_all()` parse in `python/lockless/_codegen.py`, and removed local `[tool.uv.sources]` path overrides from `pyproject.toml`.
- Updated `headerkit` (`>=0.50.0`) and `naganim` in `pyproject.toml`, updated `lockless.nimble` to `requires "lockfree >= 0.6.7 & < 0.7.0"` and `requires "naganim >= 0.1.0"`, simplified `python/lockless/_codegen.py` to use `PyiWriter(module_var="CDEF")` directly without post-processing regexes, and regenerated `python/lockless/_cdef_generated.py` and `python/lockless/_cdef_generated.pyi`.
- Unified `ll_enter_borrow` / `ll_exit_borrow` / `ll_pop_deferred_decref*` with `naganim/gc_bridge` (`naganim_enter_borrow`, `naganim_exit_borrow`, `naganim_active_borrowers`, `naganim_pop_deferred_decref_batch`), added automatic stale shared-library `st_mtime` rebuild detection in `python/lockless/_cabi.py`, removed forced `globalEpoch.fetchAdd` loops from live `trySafeReclaim` calls in `src/lockless_ext.nim` via idempotent `retirePyKVHolderOnce`, and scoped `[tool.mypy]` `mypy_path` to `["python"]` with `follow_imports = "silent"` for external sibling packages.
- Integrated `naganim` (`naganim/abi`, `naganim/gc_bridge`, `naganim/lifecycle`) and `headerkit` (`python/lockless/_codegen.py`, `python/lockless/_cdef_generated.py`, `python/lockless/_cdef_generated.pyi`) into `lockless`, removing the duplicated deferred-decref queue in `src/lockless_ext.nim`, the 211-line hand-written `ffi.cdef` string in `python/lockless/_cabi.py`, and the 3 redundant `ctypes.CDLL` handles across `channels.py`, `ratelimit.py`, and `taskpool.py`.
- Remediated concurrency, SMR, and fast-path findings (`LL-CRIT-01`, `LL-CRIT-02`, `LL-CRIT-03`, `LL-CRIT-05`, `LL-CRIT-06`, `LL-HIGH-07`, `LL-HIGH-08`):
  - Fixed `BroadcastRing` `retiredBox` reclamation in `src/lockless_ext.nim` so retired `VBox[PyBroadcastBox]` nodes are never leaked when `inUse` is set by an active subscriber.
  - Guarded DEBRA+ reclamation (`trySafeReclaim`) in `ll_ctrie_put`, `ll_ctrie_remove`, `ll_ctrie_snapshot_destroy`, `ll_skiplist_put`, and `ll_skiplist_remove` with `hasActiveDebraPin` so `globalEpoch` is never force-advanced while concurrent readers are pinned.
  - Fixed `spscPushEx`, `spscLen`, and `spscPeek` in `src/lockless_ext.nim` to use `BQueue`'s `2 * (N + 1)` virtual index space without a TOCTOU gap on `outWasEmpty`, and exposed `SpscQueue.push_ex(item) -> bool` in `queues.py` and `__init__.pyi`.
  - Eliminated mutex acquisitions on the uncontended `Channel` fast path (`_notify_after_push` and `_notify_after_pop` when 0 sync/async waiters are present) and added a dedicated high-capacity executor and 250ms timeout slices to `RendezvousChannel` to prevent threadpool starvation and short-slice livelock.
  - Fixed `TaskPool.parallel_for` 2-argument callbacks `fn(chunk_start, chunk_stop)` to dispatch true `[chunk_start, chunk_stop)` chunk ranges rather than single-element `(idx, idx + 1)` slices.
  - Guarded `StreamBuffer.reserve_write` and `StreamBuffer.peek_read` against overlapping active write/read reservations by raising `BufferError`.
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
