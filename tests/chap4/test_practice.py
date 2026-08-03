import asyncio

import pytest

from src.chap4.practice import OneShotSignal


@pytest.mark.asyncio
async def test_signal_uses_future_as_shared_state() -> None:
    signal = OneShotSignal()

    assert any(isinstance(value, asyncio.Future) for value in vars(signal).values())


@pytest.mark.asyncio
async def test_signal_delivers_value_to_all_waiters() -> None:
    signal = OneShotSignal()
    waiters = [asyncio.create_task(signal.wait()) for _ in range(3)]
    await asyncio.sleep(0)

    signal.set("ready")

    assert await asyncio.gather(*waiters) == ["ready"] * 3


@pytest.mark.asyncio
async def test_signal_delivers_failure_to_all_waiters() -> None:
    signal = OneShotSignal()
    waiters = [asyncio.create_task(signal.wait()) for _ in range(2)]
    await asyncio.sleep(0)

    signal.fail(ValueError("boom"))
    results = await asyncio.gather(*waiters, return_exceptions=True)

    assert all(
        isinstance(result, ValueError) and str(result) == "boom" for result in results
    )
    assert results[0] is results[1]


@pytest.mark.asyncio
async def test_signal_rejects_second_completion() -> None:
    signal = OneShotSignal()
    signal.set("first")

    with pytest.raises(RuntimeError):
        signal.set("second")

    with pytest.raises(RuntimeError):
        signal.fail(ValueError("late"))


@pytest.mark.asyncio
async def test_wait_after_completion_returns_stored_value() -> None:
    signal = OneShotSignal()
    signal.set("ready")

    assert await signal.wait() == "ready"


@pytest.mark.asyncio
async def test_cancelled_waiter_does_not_cancel_signal() -> None:
    signal = OneShotSignal()
    cancelled_waiter = asyncio.create_task(signal.wait())
    surviving_waiter = asyncio.create_task(signal.wait())
    await asyncio.sleep(0)

    cancelled_waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await cancelled_waiter

    signal.set("ready")
    assert await surviving_waiter == "ready"
