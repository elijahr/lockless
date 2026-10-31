"""Tests for Channel (sync + async HIGH-02), RendezvousChannel, TaskPool, and Rate Limiters."""

from __future__ import annotations

import asyncio
import threading
import time

import pytest
from lockless import (
    Channel,
    Closed,
    Empty,
    Full,
    LeakyBucket,
    RendezvousChannel,
    TaskPool,
    TokenBucket,
)


def test_channel_sync_and_negative_controls() -> None:
    ch: Channel[int] = Channel(capacity=4)
    assert ch.capacity == 4
    assert ch.is_empty is True

    for i in range(4):
        assert ch.try_send(i) is True
    assert ch.is_full is True
    assert ch.try_send(99) is False
    with pytest.raises(Full):
        ch.send_nowait(99)

    assert [ch.recv() for _ in range(4)] == [0, 1, 2, 3]
    assert ch.try_recv("empty") == "empty"
    with pytest.raises(Empty):
        ch.recv_nowait()

    ch.close()
    assert ch.is_closed is True
    with pytest.raises(Closed):
        ch.send(1)
    with pytest.raises(Closed):
        ch.recv()


def test_channel_async_for_and_0_to_1_wakeup() -> None:
    async def run_scenario() -> list[int]:
        ch: Channel[int] = Channel(capacity=128, spsc=True)
        received: list[int] = []

        def background_producer() -> None:
            time.sleep(0.01)
            for i in range(50):
                ch.send(i)
                if i % 10 == 0:
                    time.sleep(0.002)
            ch.close()

        prod_thread = threading.Thread(target=background_producer)
        prod_thread.start()

        async for item in ch:
            received.append(item)

        prod_thread.join()
        return received

    result = asyncio.run(run_scenario())
    assert result == list(range(50))


def test_rendezvous_channel_handoff_and_controls() -> None:
    rv: RendezvousChannel[str] = RendezvousChannel()
    assert rv.is_closed is False

    # Negative controls: try_send and try_recv return False / default when no peer is waiting
    assert rv.try_send("lonely") is False
    assert rv.try_recv("nobody") == "nobody"
    with pytest.raises(TimeoutError):
        rv.send("timeout-item", timeout=0.01)
    with pytest.raises(TimeoutError):
        rv.recv(timeout=0.01)

    received: list[str] = []

    def receiver_worker() -> None:
        for _ in range(5):
            received.append(rv.recv(timeout=2.0))

    t = threading.Thread(target=receiver_worker)
    t.start()
    for i in range(5):
        corr_id = rv.send(f"msg-{i}", timeout=2.0)
        assert corr_id >= 1
    t.join()

    assert received == ["msg-0", "msg-1", "msg-2", "msg-3", "msg-4"]
    rv.close()
    assert rv.is_closed is True
    with pytest.raises(Closed):
        rv.send("after-close")


def test_taskpool_submit_map_and_parallel_for() -> None:
    with TaskPool(4) as pool:
        assert pool.num_workers == 4

        fut = pool.submit(lambda a, b: a + b, 19, 23)
        assert fut.result(timeout=2.0) == 42

        mapped = pool.map(lambda x: x * x, range(16), timeout=2.0)
        assert mapped == [x * x for x in range(16)]

        # parallel_for with single-index callback
        slots = [0] * 64

        def fill_one(idx: int) -> None:
            slots[idx] = idx + 100

        pool.parallel_for(0, 64, fill_one, chunk_size=8)
        assert slots == [i + 100 for i in range(64)]

        # parallel_for with 2-arg chunk range callback (verifies real chunk ranges, LL-CRIT-06)
        chunk_slots = [0] * 64
        recorded_chunks: list[tuple[int, int]] = []
        chunk_lock = threading.Lock()

        def fill_chunk(start: int, stop: int) -> None:
            with chunk_lock:
                recorded_chunks.append((start, stop))
            for j in range(start, stop):
                chunk_slots[j] = j * 2

        pool.parallel_for(0, 64, fill_chunk, chunk_size=16)
        assert chunk_slots == [i * 2 for i in range(64)]
        assert sorted(recorded_chunks) == [(0, 16), (16, 32), (32, 48), (48, 64)]

        ragged_chunks: list[tuple[int, int]] = []

        def collect_ragged(start: int, stop: int) -> None:
            with chunk_lock:
                ragged_chunks.append((start, stop))

        pool.parallel_for(10, 25, collect_ragged, chunk_size=6)
        assert sorted(ragged_chunks) == [(10, 16), (16, 22), (22, 25)]

        # Negative control: exception propagation in submit and parallel_for
        bad_fut = pool.submit(lambda: 1 / 0)
        with pytest.raises(ZeroDivisionError):
            bad_fut.result(timeout=2.0)

        def bad_loop(idx: int) -> None:
            if idx == 7:
                raise ValueError("boom at 7")

        with pytest.raises(ValueError, match="boom at 7"):
            pool.parallel_for(0, 16, bad_loop)

        # Negative controls for parallel_for start < 0 and bool arguments
        with pytest.raises(ValueError, match="start must be non-negative"):
            pool.parallel_for(-5, 10, fill_one)
        with pytest.raises(TypeError, match="must be integers"):
            pool.parallel_for(True, 10, fill_one)  # type: ignore[arg-type]


def test_channel_async_weakref_gc_cleanup() -> None:
    import gc
    import weakref

    async def scenario() -> bool:
        ch: Channel[int] = Channel(capacity=16)
        ref = weakref.ref(ch)
        await ch.send_async(123)
        val = await ch.recv_async()
        assert val == 123
        del ch
        gc.collect()
        return ref() is None

    assert asyncio.run(scenario()) is True


def test_token_bucket_and_leaky_bucket() -> None:
    tb = TokenBucket(capacity=10, refill_rate=1000)
    assert tb.capacity == 10
    assert tb.refill_rate == 1000
    assert tb.available == 10

    assert tb.try_acquire(6) is True
    assert tb.try_acquire(6) is False
    assert tb.acquire(4, timeout=0.5) is True

    tb.reset()
    assert tb.available == 10
    tb.destroy()

    lb = LeakyBucket(capacity=5, leak_rate=1000)
    assert lb.capacity == 5
    assert lb.leak_rate == 1000
    assert lb.try_acquire(5) is True
    assert lb.try_acquire(1) is False
    assert lb.acquire(1, timeout=0.5) is True
    lb.reset()
    assert lb.water_level_ns == 0
    lb.destroy()

    # Negative controls
    with pytest.raises(ValueError):
        TokenBucket(0, 100)
    with pytest.raises(ValueError):
        LeakyBucket(10, -1)


def test_channel_uncontended_fast_path_is_lock_free() -> None:
    ch: Channel[int] = Channel(capacity=64)

    class _ForbiddenLock:
        def __enter__(self) -> None:
            raise AssertionError("Channel._lock must not be acquired on uncontended fast path")

        def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
            pass

    real_lock = ch._lock
    ch._lock = _ForbiddenLock()  # type: ignore[assignment]
    try:
        for i in range(32):
            assert ch.try_send(i) is True
        for i in range(32, 48):
            ch.send_nowait(i)
        for i in range(48, 64):
            ch.send(i)
        assert len(ch) == 64

        popped = [ch.recv_nowait() for _ in range(32)]
        popped.extend(ch.try_recv() for _ in range(16))
        popped.extend(ch.recv() for _ in range(16))
        assert popped == list(range(64))
        assert ch.is_empty is True
    finally:
        ch._lock = real_lock
        ch.destroy()


def test_rendezvous_channel_high_concurrency_async_no_starvation() -> None:
    async def scenario() -> list[int]:
        rv: RendezvousChannel[int] = RendezvousChannel()
        n = 40
        recv_tasks = [asyncio.create_task(rv.recv_async(timeout=5.0)) for _ in range(n)]
        await asyncio.sleep(0.02)
        send_tasks = [asyncio.create_task(rv.send_async(i, timeout=5.0)) for i in range(n)]

        corr_ids = await asyncio.gather(*send_tasks)
        values = await asyncio.gather(*recv_tasks)
        assert len(corr_ids) == n
        assert all(cid > 0 for cid in corr_ids)
        rv.destroy()
        return sorted(values)

    assert asyncio.run(scenario()) == list(range(40))
