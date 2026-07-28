import asyncio
import time

import pytest

from src.chap1.practice import run_blocking_safely


@pytest.mark.asyncio
async def test_returns_result() -> None:
    result = await run_blocking_safely(lambda x, y: x + y, 2, 3)
    assert result == 5


@pytest.mark.asyncio
async def test_does_not_block_heartbeat() -> None:
    ticks: list[float] = []

    async def heartbeat() -> None:
        for _ in range(3):
            ticks.append(time.perf_counter())
            await asyncio.sleep(0.05)

    await asyncio.gather(heartbeat(), run_blocking_safely(time.sleep, 0.15))

    assert len(ticks) == 3
    assert ticks[1] - ticks[0] < 0.1


@pytest.mark.asyncio
async def test_timeout() -> None:
    with pytest.raises(TimeoutError):
        await run_blocking_safely(time.sleep, 1.0, timeout=0.05)
