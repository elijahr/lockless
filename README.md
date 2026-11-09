# lockless

[![Status: Experimental](https://img.shields.io/badge/status-experimental-orange.svg)](#status)

> [!WARNING]
> **Experimental / Pre-Alpha Software — Not Ready for Production**
> This project is an active research and development prototype. APIs, ABI layouts, and wire formats are unstable and subject to breaking changes without notice. Do not use in production systems.

Lock-free concurrent data structures and zero-copy DSP primitives for Python 3.10+ (standard CPython and free-threaded PEP 703 `python3.13t` / `python3.14t`), backed by [`lockfree`](https://github.com/axiomantic/lockfree).

## Primitives

- **Queues**: `BQueue128`, `SpscQueue`, `MpmcQueue`
- **Zero-Copy DSP Stream Buffer**: `StreamBuffer` (`reserve_write`, `peek_read`, `(first_view, second_view)` `memoryview` pairs)
- **Pub/Sub Broadcast Ring**: `BroadcastRing`, `Subscriber`
- **Associative Containers**: `ConcurrentDict` (`Ctrie`), `SkipListMap`, `SkipListSet`
- **Sync + Async Channels**: `Channel`, `RendezvousChannel` (`async for item in ch:`)
- **Work-Stealing Thread Pool**: `TaskPool`
- **Rate Limiters**: `TokenBucket`, `LeakyBucket`
