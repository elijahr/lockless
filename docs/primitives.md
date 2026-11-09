# `lockless` Primitives Guide & API Reference

> [!WARNING]
> **Experimental / Pre-Alpha Software — Not Ready for Production**
> This project is an active research and development prototype. APIs, ABI layouts, and wire formats are unstable and subject to breaking changes without notice. Do not use in production systems.

Complete guide and API reference for every primitive exported by `lockless`.

---

## 1. Exceptions & Diagnostics (`lockless`)

- **`LocklessError(Exception)`**: Base exception class for all `lockless` errors.
- **`Empty(LocklessError)`**: Raised by non-blocking `pop()`, `get_nowait()`, `recv_nowait()`, or `poll()` when a queue, channel, or broadcast ring has no available items, or when `recv(timeout)` expires on `Channel`.
- **`Full(LocklessError)`**: Raised by non-blocking `push()`, `put_nowait()`, `send_nowait()`, `publish()`, or `reserve_write()` when a bounded container is full, or when `send(timeout)` expires.
- **`Closed(LocklessError)`**: Raised when an operation is attempted on a closed or destroyed container.
- **`LaggedError(LocklessError)`**: Raised by `Subscriber.poll(raise_on_lag=True)` when a `BroadcastRing` in `"drop_oldest"` mode overwrote unread messages before the subscriber polled them. Exposes `.skipped_count: int`.
- **`drain_deferred_decrefs() -> int`**: Explicitly drains all pending retired `PyObject*` pointers from the `naganim/gc_bridge` wait-free retirement queue and calls `Py_DecRef` under the GIL. Returns the number of reclaimed objects.

---

## 2. Queues (`lockless.queues`)

### 2.1 `BQueue128[T]`
Wait-free Single-Producer Single-Consumer (SPSC) ring buffer using 128-bit atomic batch probing.

- **`BQueue128(capacity: int = 1024)`** — `capacity` must be a power of 2 in `[128, 16384]`.
- **Properties**: `.capacity -> int`, `.is_closed -> bool`, `.is_empty -> bool`, `.is_full -> bool`, `len(q) -> int`.
- **Methods**:
  - `push(item: T) -> None` (alias `put_nowait`): Enqueues `item` or raises `Full` / `Closed`.
  - `push_ex(item: T) -> bool`: Enqueues `item` and returns `True` if and only if the queue transitioned from `0` to `1` item (used for edge-triggered wakeups).
  - `try_push(item: T) -> bool`: Returns `True` if enqueued, `False` if full.
  - `pop() -> T` (alias `get_nowait`): Dequeues and returns the head item or raises `Empty` / `Closed`.
  - `try_pop(default: Any = None) -> Any`: Dequeues and returns the head item, or `default` if empty.
  - `pop_batch(max_count: int = 64) -> list[T]`: Dequeues up to `max_count` items.
  - `clear() -> None`: Drains all buffered items and releases their Python reference counts.
  - `close() -> None`: Marks the queue closed to new pushes while allowing remaining items to be popped.
  - `destroy() -> None`: Destroys the native queue and reclaims all remaining items.

### 2.2 `SpscQueue[T]`
Wait-free Lamport Single-Producer Single-Consumer ring buffer using a `2 * (N + 1)` virtual index space.

- **`SpscQueue(capacity: int = 1024)`** — `capacity` must be a power of 2 in `[2, 65536]`.
- **Properties & Methods**: Same API as `BQueue128[T]` (`capacity`, `is_closed`, `is_empty`, `is_full`, `__len__`, `push`, `push_ex`, `try_push`, `put_nowait`, `pop`, `get_nowait`, `try_pop`, `pop_batch`, `clear`, `close`, `destroy`). `pop_batch` executes a single C-ABI batch call (`ll_spsc_pop_batch`).

### 2.3 `MpmcQueue[T]`, `MpmcProducer[T]`, `MpmcConsumer[T]`
Lock-free Multi-Producer Multi-Consumer (and MPSC / SPMC) queue:
- When `capacity` is a power of 2 in `[2, 65536]`, uses a bounded Vyukov MPMC ring.
- When `capacity=None` (or `0`), uses an unbounded lock-free Michael-Scott queue with DEBRA+ epoch-based safe memory reclamation.

- **`MpmcQueue(capacity: int | None = 1024)`**
- **Properties**: `.capacity -> int | None`, `.is_closed -> bool`, `.is_empty -> bool`, `.is_full -> bool`, `len(q) -> int`.
- **Handle Factories**:
  - `producer() -> MpmcProducer[T]`: Acquires an explicit producer handle (`push`, `push_ex`, `try_push`, `close`).
  - `consumer() -> MpmcConsumer[T]`: Acquires an explicit consumer handle (`pop`, `try_pop`, `pop_batch`, `close`).
- **Direct Queue Methods** (using per-thread default handles automatically):
  `push`, `push_ex`, `try_push`, `put_nowait`, `pop`, `get_nowait`, `try_pop`, `pop_batch`, `clear`, `close`, `destroy`.

---

## 3. Zero-Copy Byte Stream Ring (`lockless.streambuffer`)

### 3.1 `StreamBuffer`
Wait-free SPSC byte ring buffer exposing zero-copy bipartite `memoryview` reservations (`(first_view, second_view)`).

- **`StreamBuffer(capacity: int = 4096, virtual_mirror: bool = False)`** — `capacity` must be a power of 2 `>= 2`.
- **Properties**: `.capacity -> int`, `.is_closed -> bool`, `.available_read -> int`, `.available_write -> int`, `.is_empty -> bool`, `.is_full -> bool`, `len(sb) -> int`.
- **Zero-Copy Two-Phase API**:
  - `reserve_write(nbytes: int) -> StreamWriteReservation`: Reserves `nbytes` for writing. Raises `Full` if `available_write < nbytes` or `BufferError` if another write reservation is already active.
    When used as `with sb.reserve_write(n) as (v1, v2):`, automatically commits `n` bytes and releases `v1`/`v2` on normal exit, or aborts without committing if an exception is raised.
  - `commit_write(nbytes: int) -> None`: Releases active write views and advances the write cursor by `nbytes`.
  - `peek_read(max_bytes: int | None = None) -> StreamReadReservation`: Peeks at up to `max_bytes` (or all readable bytes) as read-only `(v1, v2)` `memoryview` slices. Raises `BufferError` if another read reservation is active.
  - `consume_read(nbytes: int) -> None`: Releases active read views and advances the read cursor by `nbytes`.
- **Convenience Copy/Buffer API**:
  - `write(data: bytes | bytearray | memoryview) -> int`
  - `read(max_bytes: int | None = None) -> bytes`
  - `readinto(buf: bytearray | memoryview) -> int`
  - `close() -> None` / `destroy() -> None` (raises `BufferError` if any zero-copy reservation views remain unreleased).

---

## 4. Synchronous & `asyncio` Channels (`lockless.channels`)

### 4.1 `Channel[T]`
Hybrid synchronous and `asyncio` channel combining a lock-free queue (`MpmcQueue` by default, or wait-free `BQueue128` when `spsc=True` and `128 <= capacity <= 16384`) with a 0-to-1 OS wakeup pipe and Dekker store-load fence.

- **`Channel(capacity: int | None = 1024, *, spsc: bool = False)`**
- **Non-Blocking Methods**:
  - `send_nowait(item: T) -> None` / `try_send(item: T) -> bool`
  - `recv_nowait() -> T` / `try_recv(default: Any = None) -> Any`
- **Synchronous Blocking Methods**:
  - `send(item: T, timeout: float | None = None) -> None` (raises `Full` on timeout, `Closed` if closed)
  - `recv(timeout: float | None = None) -> T` (raises `Empty` on timeout, `Closed` when closed and drained)
  - `for item in ch:` — Iterates synchronously until the channel is closed and drained.
- **`asyncio` Methods**:
  - `await send_async(item: T) -> None`
  - `await recv_async() -> T`
  - `async for item in ch:` — Asynchronously iterates via `loop.add_reader` wakeup pipe with zero polling timers.
- **Lifecycle**: `close() -> None`, `destroy() -> None`.

### 4.2 `RendezvousChannel[T]`
Zero-capacity synchronous/asynchronous handoff channel where every sender pairs directly with a receiver.

- **`RendezvousChannel()`**
- **Methods**:
  - `try_send(item: T) -> bool`: Immediate handoff if a receiver is already waiting, else `False`.
  - `try_recv(default: Any = None) -> Any`: Immediate handoff if a sender is already waiting, else `default`.
  - `send(item: T, timeout: float | None = None) -> int`: Blocks (releasing the GIL in 250ms native slices) until paired with a receiver and returns the 64-bit correlation ID. Raises `TimeoutError` on timeout.
  - `recv(timeout: float | None = None) -> T`: Blocks (releasing the GIL) until paired with a sender and returns the item. Raises `TimeoutError` on timeout.
  - `await send_async(item: T, timeout: float | None = None) -> int`
  - `await recv_async(timeout: float | None = None) -> T`
  - `close() -> None`, `destroy() -> None`.

---

## 5. Lock-Free Maps, Sets & Pub/Sub (`lockless.maps`)

### 5.1 `ConcurrentDict[K, V]` (alias `Ctrie`) & `ConcurrentDictSnapshot[K, V]`
Lock-free concurrent hash array mapped trie supporting arbitrary hashable keys (`int`, arbitrary-precision big `int`, `float`, `bool`, `str`, `bytes`, nested scalar `tuple`, and custom hashable objects) with $O(1)$ linearizable snapshots.

- **`ConcurrentDict(initial: dict[K, V] | None = None)`**
- **Mutation & Lookup Methods**:
  - `put(key: K, value: V) -> V | None` (returns displaced value or `None`)
  - `put_if_absent(key: K, value: V) -> V` (alias `setdefault(key, default)`)
  - `compute_if_absent(key: K, factory: Callable[[K], V]) -> V`
  - `get(key: K, default: Any = None) -> Any`
  - `remove(key: K) -> V` / `pop(key: K[, default]) -> Any`
  - `d[key]`, `d[key] = value`, `del d[key]`, `key in d`, `len(d)`
  - `keys() -> list[K]`, `values() -> list[V]`, `items() -> list[tuple[K, V]]`, `clear() -> None`
- **$O(1)$ Point-in-Time Snapshot**:
  - `snapshot() -> ConcurrentDictSnapshot[K, V]` (supports `get`, `snap[k]`, `k in snap`, `len(snap)`, `keys()`, `values()`, `items()`, `to_dict()`, `close()`).

### 5.2 `SkipListMap[K, V]`
Lock-free ordered map maintaining keys in ascending order using zero-pickle lexicographical key encoding.

- **`SkipListMap(initial: dict[K, V] | None = None)`**
- **Methods**:
  - `put(key: K, value: V) -> bool` (returns `True` if newly inserted, `False` if updated)
  - `put_if_absent(key: K, value: V) -> V` (alias `setdefault`)
  - `compute_if_absent(key: K, factory: Callable[[K], V]) -> V`
  - `get(key: K, default: Any = None) -> Any`
  - `remove(key: K) -> V` / `pop(key: K[, default]) -> Any`
  - `m[key]`, `m[key] = value`, `del m[key]`, `key in m`, `len(m)`
  - `keys() -> list[K]`, `values() -> list[V]`, `items() -> list[tuple[K, V]]` (all returned in ascending key order)
  - `clear() -> None`, `close() -> None`, `destroy() -> None`.

### 5.3 `SkipListSet[T]`
Lock-free ordered set maintaining elements in ascending order.

- **`SkipListSet(initial: Iterable[T] | None = None)`**
- **Methods**:
  - `add(item: T) -> bool` (returns `True` if newly added)
  - `remove(item: T) -> None` (raises `KeyError` if absent)
  - `discard(item: T) -> bool` (returns `True` if removed, `False` if absent)
  - `item in s`, `len(s)`, `iter(s)`, `to_list() -> list[T]` (ascending order)
  - `clear() -> None`, `close() -> None`, `destroy() -> None`.

### 5.4 `BroadcastRing[T]` & `Subscriber[T]` (alias `BroadcastSubscriber`)
Multi-subscriber pub/sub ring buffer where every attached subscriber cursor receives every published item.

- **`BroadcastRing(capacity: int = 1024, overflow: str = "drop_oldest", max_readers: int = 16)`**
  - `capacity`: Power of 2 in `[16, 65536]`.
  - `overflow`: `"drop_oldest"` (wait-free overwrite of oldest unread slot) or `"backpressure"` / `"reject"` (never overwrite unread slots; `publish` raises `Full` and `try_publish` returns `False`).
  - `max_readers`: Integer in `[1, 64]`.
- **`BroadcastRing` Methods**:
  - `publish(item: T) -> None`, `try_publish(item: T) -> bool`
  - `subscribe(*, from_earliest: bool = False) -> Subscriber[T]`
  - `.capacity -> int`, `.subscriber_count -> int`, `.is_closed -> bool`, `len(ring) -> int`
  - `close() -> None`, `destroy() -> None`
- **`Subscriber` Methods**:
  - `poll(*, raise_on_lag: bool = False) -> T`: Polls next item or raises `Empty` / `Closed` (or `LaggedError` when `raise_on_lag=True`).
  - `try_poll(default: Any = None) -> Any`
  - `recv(timeout: float | None = None) -> T`
  - `.lag -> int`, `.total_skipped -> int`, `.is_closed -> bool`
  - `unsubscribe() -> None` (alias `close()`).

---

## 6. Work-Stealing Thread Pool (`lockless.taskpool`)

### `TaskPool`
Chase-Lev work-stealing native thread pool.

- **`TaskPool(num_workers: int = 0)`** (`0` auto-detects CPU core count).
- **Properties & Methods**:
  - `.num_workers -> int`, `.is_closed -> bool`
  - `submit(fn: Callable[..., R], /, *args, **kwargs) -> concurrent.futures.Future[R]`
  - `map(fn: Callable[[T], R], iterable: Iterable[T], *, timeout: float | None = None) -> list[R]`
  - `parallel_for(start: int, stop: int, fn: Callable[..., Any], *, chunk_size: int = 0) -> None` — Supports both 1-arg `fn(i)` and 2-arg chunk callbacks `fn(chunk_start, chunk_stop)`.
  - `sync() -> None` — Blocks (releasing the GIL) until all in-flight tasks finish.
  - `shutdown(wait: bool = True) -> None` (aliases `close`, `destroy`).

---

## 7. Lock-Free Rate Limiters (`lockless.ratelimit`)

### 7.1 `TokenBucket`
Lock-free 128-bit CAS token bucket rate limiter.

- **`TokenBucket(capacity: int, refill_rate: int)`**
- **Properties**: `.capacity -> int`, `.refill_rate -> int` (alias `.rate`), `.available -> int`, `.is_closed` via `close()`.
- **Methods**:
  - `try_acquire(tokens: int = 1) -> bool` (alias `try_consume`)
  - `acquire(tokens: int = 1, timeout: float | None = None) -> bool` (alias `acquire_timeout`) — Releases the GIL while waiting up to `timeout` seconds.
  - `reset() -> None`, `close() -> None`, `destroy() -> None`.

### 7.2 `LeakyBucket`
Lock-free Generic Cell Rate Algorithm (GCRA) leaky bucket rate meter.

- **`LeakyBucket(capacity: int, leak_rate: int)`**
- **Properties**: `.capacity -> int`, `.leak_rate -> int` (alias `.rate`), `.water_level_ns -> int`, `.burst_tolerance_ns -> int`.
- **Methods**:
  - `try_acquire(units: int = 1) -> bool` (alias `try_consume`)
  - `acquire(units: int = 1, timeout: float | None = None) -> bool` (alias `acquire_timeout`)
  - `reset() -> None`, `close() -> None`, `destroy() -> None`.
