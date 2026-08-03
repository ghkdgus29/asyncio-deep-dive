import asyncio

import pytest

from src.chap6.practice import fetch_user_profile


@pytest.mark.asyncio
async def test_profile_fetches_all_services_concurrently() -> None:
    started_count = 0
    all_started = asyncio.Event()

    async def fetch(value: object) -> object:
        nonlocal started_count
        started_count += 1
        if started_count == 3:
            all_started.set()
        await asyncio.wait_for(all_started.wait(), timeout=0.5)
        return value

    result = await fetch_user_profile(
        1,
        fetch_user=lambda _: fetch({"id": 1}),
        fetch_orders=lambda _: fetch([1, 2]),
        fetch_preferences=lambda _: fetch({"theme": "dark"}),
        timeout=1,
    )

    assert started_count == 3
    assert result == {
        "user": {"id": 1},
        "orders": [1, 2],
        "preferences": {"theme": "dark"},
    }


@pytest.mark.asyncio
async def test_profile_failure_cancels_siblings() -> None:
    cancelled = [asyncio.Event(), asyncio.Event()]

    async def long_running(index: int) -> None:
        try:
            await asyncio.sleep(10)
        finally:
            cancelled[index].set()

    async def fail(_: int) -> None:
        raise RuntimeError("downstream failed")

    with pytest.raises(ExceptionGroup):
        await fetch_user_profile(
            1,
            fetch_user=fail,
            fetch_orders=lambda _: long_running(0),
            fetch_preferences=lambda _: long_running(1),
            timeout=1,
        )

    assert all(event.is_set() for event in cancelled)


@pytest.mark.asyncio
async def test_profile_applies_overall_timeout() -> None:
    async def slow(_: int) -> None:
        await asyncio.sleep(10)

    with pytest.raises(TimeoutError):
        await fetch_user_profile(
            1,
            fetch_user=slow,
            fetch_orders=slow,
            fetch_preferences=slow,
            timeout=0.01,
        )
