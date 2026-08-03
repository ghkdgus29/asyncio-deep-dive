import asyncio


async def trace_schedule(worker_count: int, steps: int) -> list[str]:
    """지정한 worker 수와 step 수만큼 Task를 실행하고 모든 실행 이벤트를 반환한다.

    Args:
        worker_count: worker 수
        steps: 각 worker의 step 수

    Returns:
        실행 이벤트의 문자열 목록

    Raises:
        ValueError: 음수 worker 수 또는 step 수
    """
    # TODO: 음수 입력 검증
    # TODO: 각 worker가 steps만큼 반복하며 이벤트 기록
    # TODO: 각 step 후 await asyncio.sleep(0)으로 제어권 양보
    # TODO: asyncio.gather로 모든 worker를 동시에 실행

    if worker_count < 0 or steps < 0:
        raise ValueError

    events = []

    async def worker(idx: int):
        for step_idx in range(steps):
            events.append(f"worker-{idx}:step-{step_idx}")
            await asyncio.sleep(0)

    await asyncio.gather(*[worker(i) for i in range(worker_count)])
    return events
