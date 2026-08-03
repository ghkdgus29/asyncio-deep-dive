import pytest

from src.chap3.practice import trace_schedule


@pytest.mark.asyncio
async def test_trace_schedule_contains_every_event() -> None:
    events = await trace_schedule(worker_count=3, steps=2)

    assert len(events) == 6
    assert set(events) == {
        "worker-0:step-0",
        "worker-0:step-1",
        "worker-1:step-0",
        "worker-1:step-1",
        "worker-2:step-0",
        "worker-2:step-1",
    }


@pytest.mark.asyncio
async def test_trace_schedule_preserves_each_worker_order() -> None:
    events = await trace_schedule(worker_count=3, steps=3)

    for worker_id in range(3):
        positions = [
            events.index(f"worker-{worker_id}:step-{step}") for step in range(3)
        ]
        assert positions == sorted(positions)


@pytest.mark.asyncio
async def test_trace_schedule_yields_after_each_step() -> None:
    events = await trace_schedule(worker_count=3, steps=2)

    assert set(events[:3]) == {
        "worker-0:step-0",
        "worker-1:step-0",
        "worker-2:step-0",
    }
    assert set(events[3:]) == {
        "worker-0:step-1",
        "worker-1:step-1",
        "worker-2:step-1",
    }


@pytest.mark.asyncio
async def test_trace_schedule_handles_empty_work() -> None:
    assert await trace_schedule(worker_count=0, steps=3) == []
    assert await trace_schedule(worker_count=3, steps=0) == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("worker_count", "steps"),
    [(-1, 1), (1, -1)],
)
async def test_trace_schedule_rejects_negative_arguments(
    worker_count: int,
    steps: int,
) -> None:
    with pytest.raises(ValueError):
        await trace_schedule(worker_count=worker_count, steps=steps)
