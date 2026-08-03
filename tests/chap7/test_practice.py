import asyncio

import pytest

from src.chap7.practice import WorkerPool


@pytest.mark.asyncio
async def test_worker_pool_limits_concurrency() -> None:
    active = 0
    maximum = 0
    processed: set[int] = set()

    async def handler(item: int) -> None:
        nonlocal active, maximum
        active += 1
        maximum = max(maximum, active)
        try:
            await asyncio.sleep(0.01)
            processed.add(item)
        finally:
            active -= 1

    pool = WorkerPool(worker_count=3, queue_size=5, handler=handler)
    await pool.start()
    try:
        for item in range(20):
            await pool.submit(item)
        await pool.join()
    finally:
        await pool.close()

    assert maximum == 3
    assert processed == set(range(20))


@pytest.mark.asyncio
async def test_worker_pool_applies_submit_backpressure() -> None:
    gate = asyncio.Event()

    async def handler(item: int) -> None:
        await gate.wait()

    pool = WorkerPool(worker_count=1, queue_size=1, handler=handler)
    await pool.start()

    try:
        await pool.submit(1)
        await pool.submit(2)
        blocked_submit = asyncio.create_task(pool.submit(3))
        await asyncio.sleep(0)

        assert not blocked_submit.done()

        gate.set()
        await blocked_submit
        await pool.join()
    finally:
        gate.set()
        await pool.close()


@pytest.mark.asyncio
async def test_worker_pool_rejects_submit_after_close() -> None:
    async def handler(item: int) -> None:
        return None

    pool = WorkerPool(worker_count=1, queue_size=1, handler=handler)
    await pool.start()
    await pool.close()

    with pytest.raises(RuntimeError):
        await pool.submit(1)
