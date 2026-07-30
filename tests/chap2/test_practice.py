import asyncio
import time

import pytest

from src.chap2.practice import (
    DelayResult,
    NextLoop,
    trace_coroutine_lifecycle,
)


@pytest.mark.asyncio
async def test_traces_coroutine_lifecycle() -> None:
    assert await trace_coroutine_lifecycle() == [
        "CORO_CREATED",
        "CORO_SUSPENDED",
        "CORO_CLOSED",
    ]


def test_next_loop_implements_await_protocol_directly() -> None:
    assert "__await__" in NextLoop.__dict__
    # assert "__await__" in DelayResult.__dict__


@pytest.mark.asyncio
async def test_next_loop_yields_control_once() -> None:
    events: list[str] = []

    async def worker(name: str) -> None:
        events.append(f"{name}:before")
        await NextLoop()
        events.append(f"{name}:after")

    await asyncio.gather(worker("A"), worker("B"))

    assert set(events[:2]) == {"A:before", "B:before"}
    assert set(events[2:]) == {"A:after", "B:after"}


@pytest.mark.asyncio
async def test_next_loop_resumes_on_next_loop_turn() -> None:
    async def wait_once() -> None:
        await NextLoop()

    task = asyncio.create_task(wait_once())
    await asyncio.sleep(0)
    assert not task.done()

    await asyncio.sleep(0)
    assert task.done()


@pytest.mark.asyncio
async def test_delay_result_returns_value() -> None:
    assert await DelayResult(value=42, delay=0.01) == 42


@pytest.mark.asyncio
async def test_delay_result_preserves_value_type() -> None:
    value = {"status": "done"}

    assert await DelayResult(value=value, delay=0) is value


@pytest.mark.asyncio
async def test_delay_result_waits_for_delay() -> None:
    started = time.perf_counter()
    await DelayResult(value=None, delay=0.05)

    assert time.perf_counter() - started >= 0.04


def test_delay_result_rejects_negative_delay() -> None:
    with pytest.raises(ValueError):
        DelayResult(value="invalid", delay=-1)


@pytest.mark.asyncio
async def test_delay_results_run_concurrently() -> None:
    started = time.perf_counter()

    results = await asyncio.gather(
        DelayResult(value="A", delay=0.1),
        DelayResult(value="B", delay=0.1),
    )

    assert results == ["A", "B"]
    assert time.perf_counter() - started < 0.18


@pytest.mark.asyncio
async def test_delay_result_preserves_cancellation() -> None:
    async def wait_for_result() -> str:
        return await DelayResult(value="late", delay=10)

    task = asyncio.create_task(wait_for_result())
    await asyncio.sleep(0)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task
