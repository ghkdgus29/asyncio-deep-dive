import asyncio
import inspect
from typing import Any


async def trace_coroutine_lifecycle() -> list[str]:
    """worker coroutine의 상태를 생성 직후, 중단 시점, 완료 후 순서로 반환한다.

    Returns:
        ["CORO_CREATED", "CORO_SUSPENDED", "CORO_CLOSED"]
    """
    states: list[str] = []

    async def worker() -> None:
        # TODO: 여기서 await를 사용하여 상태를 전환
        await asyncio.sleep(2)

    # TODO: 코루틴 객체 생성 및 상태 추적
    # 1. 코루틴 생성 -> CORO_CREATED 기록
    # 2. Task로 감싸고 await asyncio.sleep(0) -> CORO_SUSPENDED 기록
    # 3. Task 완료 대기 -> CORO_CLOSED 기록
    coro = worker()
    states.append(inspect.getcoroutinestate(coro))
    task = asyncio.create_task(coro)
    await asyncio.sleep(0)
    states.append(inspect.getcoroutinestate(coro))
    await task
    states.append(inspect.getcoroutinestate(coro))

    return states


class NextLoop:
    """이벤트 루프에 정확히 한 번 제어권을 양보하는 awaitable."""

    def __await__(self):
        # TODO: yield를 사용하여 이벤트 루프에 제어권 양보
        # asyncio.sleep(0)의 내부 구현 참고
        yield


class DelayResult:
    """delay 이후 value를 반환하는 awaitable."""

    def __init__(self, value: Any, delay: float) -> None:
        # TODO: delay가 음수면 ValueError 발생
        # TODO: value와 delay 저장
        if delay < 0:
            raise ValueError

        self._value = value
        self._delay = delay

    def __await__(self):
        # TODO: timer를 사용하여 delay 동안 대기
        # TODO: 완료 후 value 반환
        # asyncio.get_event_loop().call_later 사용
        loop = asyncio.get_event_loop()
        future = loop.create_future()
        loop.call_later(self._delay, future.set_result, self._value)
        return (yield from future.__await__())
