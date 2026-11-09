# `lockless` Documentation

> [!WARNING]
> **Experimental / Pre-Alpha Software — Not Ready for Production**
> This project is an active research and development prototype. APIs, ABI layouts, and wire formats are unstable and subject to breaking changes without notice. Do not use in production systems.

`lockless` provides wait-free and lock-free concurrency and zero-copy DSP primitives for **Python 3.11+** and **Free-Threaded Python (`python3.13t` / `python3.14t` PEP 703)**, powered by [`elijahr/lockfree`](https://github.com/elijahr/lockfree), [`elijahr/naganim`](https://github.com/elijahr/naganim), and [`axiomantic/headerkit`](https://github.com/axiomantic/headerkit).

---

## Documentation Guide

1. [**Primitives Guide & Complete API Reference (`primitives.md`)**](primitives.md)
   - Queues (`BQueue128`, `SpscQueue`, `MpmcQueue`, `MpmcProducer`, `MpmcConsumer`)
   - Zero-Copy Byte Stream Ring (`StreamBuffer`, `StreamWriteReservation`, `StreamReadReservation`)
   - Synchronous & `asyncio` Channels (`Channel`, `RendezvousChannel`)
   - Lock-Free Maps, Sets & Pub/Sub (`ConcurrentDict` / `Ctrie`, `ConcurrentDictSnapshot`, `SkipListMap`, `SkipListSet`, `BroadcastRing`, `Subscriber`)
   - Work-Stealing Pool (`TaskPool`)
   - Lock-Free Rate Limiters (`TokenBucket`, `LeakyBucket`)
   - Exceptions & Diagnostics (`LocklessError`, `Empty`, `Full`, `Closed`, `LaggedError`, `drain_deferred_decrefs`)
2. [**Architecture & C-ABI Bridge (`architecture.md`)**](architecture.md)
   - How `lockless` bridges `elijahr/lockfree` via `naganim` (`gc_bridge` borrow guards & wait-free deferred `Py_DecRef`) and `headerkit` (`NimBackend` C-ABI codegen)
   - Zero-pickle lexicographical key encoding for `ConcurrentDict`, `SkipListMap`, and `SkipListSet`
   - Zero-polling 0-to-1 OS pipe wakeup and Dekker store-load fence in `Channel`

---

## Quickstart

```python
from lockless import (
    BQueue128,
    BroadcastRing,
    Channel,
    ConcurrentDict,
    LeakyBucket,
    MpmcQueue,
    RendezvousChannel,
    SkipListMap,
    SkipListSet,
    SpscQueue,
    StreamBuffer,
    TaskPool,
    TokenBucket,
)

# Wait-free SPSC queue
with BQueue128[str](capacity=128) as q:
    q.push("hello")
    assert q.pop() == "hello"

# Lock-free Ctrie with O(1) point-in-time snapshot
with ConcurrentDict[str, int]({"workers": 8}) as d:
    with d.snapshot() as snap:
        d["workers"] = 16
        assert snap["workers"] == 8
        assert d["workers"] == 16
```
