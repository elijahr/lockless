"""Tests for BQueue128, SpscQueue, and MpmcQueue."""

from __future__ import annotations

import gc
import sys
import threading

import pytest
from lockless import (
    BQueue128,
    Closed,
    Empty,
    Full,
    MpmcQueue,
    SpscQueue,
    drain_deferred_decrefs,
)


class _TrackObj:
    def __init__(self, val: int) -> None:
        self.val = val


def test_bqueue128_basic_and_push_ex() -> None:
    q: BQueue128[object] = BQueue128(128)
    assert q.capacity == 128
    assert q.is_empty is True
    assert q.is_full is False
    assert q.is_closed is False
    assert len(q) == 0

    # 0-to-1 transition returns True on push_ex
    assert q.push_ex("first") is True
    assert q.push_ex("second") is False
    assert q.push_ex(None) is False
    assert len(q) == 3
    assert q.is_empty is False

    assert q.pop() == "first"
    assert q.pop() == "second"
    assert q.pop() is None
    assert q.is_empty is True

    # Next push after draining to 0 is another 0-to-1 transition
    assert q.push_ex(42) is True
    assert q.pop() == 42


def test_bqueue128_batch_and_full_controls() -> None:
    q: BQueue128[int] = BQueue128(128)
    for i in range(128):
        assert q.try_push(i) is True
    assert q.is_full is True
    assert len(q) == 128

    # Negative control: pushing when full
    assert q.try_push(999) is False
    with pytest.raises(Full):
        q.push(999)

    batch1 = q.pop_batch(50)
    assert batch1 == list(range(50))
    batch2 = q.pop_batch(100)
    assert batch2 == list(range(50, 128))
    assert q.is_empty is True

    # Negative control: popping when empty
    assert q.try_pop("sentinel") == "sentinel"
    with pytest.raises(Empty):
        q.pop()

    q.close()
    assert q.is_closed is True
    with pytest.raises(Closed):
        q.push(1)
    with pytest.raises(Closed):
        q.pop()


def test_bqueue128_invalid_capacities() -> None:
    for bad_cap in (0, 1, 64, 100, 129, 32768, -128):
        with pytest.raises(ValueError):
            BQueue128(bad_cap)
    with pytest.raises(TypeError):
        BQueue128(True)  # type: ignore[arg-type]


def test_spsc_queue_lifecycle_and_refcounts() -> None:
    q: SpscQueue[_TrackObj] = SpscQueue(4)
    assert q.capacity == 4
    items = [_TrackObj(i) for i in range(4)]
    baseline_refs = [sys.getrefcount(obj) for obj in items]

    for idx in range(len(items)):
        q.push(items[idx])
    assert q.is_full is True

    # Negative control: rejected push on full SpscQueue does not leak refcount
    extra = _TrackObj(99)
    extra_base = sys.getrefcount(extra)
    assert q.try_push(extra) is False
    with pytest.raises(Full):
        q.push(extra)
    assert sys.getrefcount(extra) == extra_base

    popped0 = q.pop()
    assert popped0.val == 0
    del popped0

    # Clear remaining 3 items and verify their refcounts return to baseline
    q.clear()
    drain_deferred_decrefs()
    gc.collect()
    assert [sys.getrefcount(obj) for obj in items] == baseline_refs


def test_spsc_queue_invalid_capacities() -> None:
    for bad_cap in (0, 1, 3, 5, 100, 131072, -4):
        with pytest.raises(ValueError):
            SpscQueue(bad_cap)
    with pytest.raises(TypeError):
        SpscQueue("16")  # type: ignore[arg-type]


def test_mpmc_queue_bounded_and_unbounded() -> None:
    bq: MpmcQueue[int] = MpmcQueue(4)
    assert bq.capacity == 4
    assert bq.push_ex(10) is True
    assert bq.push_ex(20) is False
    assert bq.try_push(30) is True
    assert bq.try_push(40) is True
    assert bq.try_push(50) is False
    with pytest.raises(Full):
        bq.push(50)
    assert bq.pop_batch(10) == [10, 20, 30, 40]
    assert bq.is_empty is True

    # Unbounded MpmcQueue
    uq: MpmcQueue[int] = MpmcQueue(None)
    assert uq.capacity is None
    assert uq.is_full is False
    for i in range(300):
        uq.push(i)
    assert len(uq) == 300
    out = uq.pop_batch(300)
    assert out == list(range(300))
    assert uq.is_empty is True


def test_mpmc_multithreaded_contention() -> None:
    q: MpmcQueue[tuple[int, int]] = MpmcQueue(1024)
    num_producers = 4
    num_consumers = 4
    items_per_producer = 250
    total_items = num_producers * items_per_producer
    collected: list[tuple[int, int]] = []
    lock = threading.Lock()

    def producer_worker(pid: int) -> None:
        with q.producer() as prod:
            for i in range(items_per_producer):
                while not prod.try_push((pid, i)):
                    pass

    def consumer_worker() -> None:
        local: list[tuple[int, int]] = []
        with q.consumer() as cons:
            while True:
                item = cons.try_pop()
                if item is not None:
                    local.append(item)
                elif q.is_closed and q.is_empty:
                    # Final drain check
                    rem = cons.try_pop()
                    if rem is not None:
                        local.append(rem)
                    else:
                        break
        with lock:
            collected.extend(local)

    p_threads = [threading.Thread(target=producer_worker, args=(p,)) for p in range(num_producers)]
    c_threads = [threading.Thread(target=consumer_worker) for _ in range(num_consumers)]

    for t in p_threads + c_threads:
        t.start()
    for t in p_threads:
        t.join()
    q.close()
    for t in c_threads:
        t.join()

    expected = sorted((p, i) for p in range(num_producers) for i in range(items_per_producer))
    assert len(collected) == total_items
    assert sorted(collected) == expected


def test_unbounded_mpmc_direct_multithreaded_and_handle_invalidation() -> None:
    uq: MpmcQueue[tuple[int, int]] = MpmcQueue(None)
    prod_handle = uq.producer()
    cons_handle = uq.consumer()

    num_threads = 4
    items_per_thread = 100

    # Concurrent threads calling uq.push() and uq.try_pop() directly (using per-thread TLS handles)
    def push_worker(tid: int) -> None:
        for i in range(items_per_thread):
            uq.push((tid, i))

    threads = [threading.Thread(target=push_worker, args=(t,)) for t in range(num_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    popped: list[tuple[int, int]] = []
    pop_lock = threading.Lock()

    def pop_worker() -> None:
        local: list[tuple[int, int]] = []
        while True:
            item = uq.try_pop()
            if item is None:
                break
            local.append(item)
        with pop_lock:
            popped.extend(local)

    pop_threads = [threading.Thread(target=pop_worker) for _ in range(num_threads)]
    for t in pop_threads:
        t.start()
    for t in pop_threads:
        t.join()

    expected = sorted((t, i) for t in range(num_threads) for i in range(items_per_thread))
    assert sorted(popped) == expected

    # Destroying the queue must close all open producer/consumer handles
    uq.destroy()
    with pytest.raises(Closed):
        prod_handle.push((0, 0))
    with pytest.raises(Closed):
        cons_handle.pop()
    prod_handle.close()
    cons_handle.close()
