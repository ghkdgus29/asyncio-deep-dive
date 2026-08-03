import asyncio
from typing import Any


class OneShotSignal:
    """여러 waiter에게 한 번만 값 또는 예외를 전달하는 signal."""

    def __init__(self) -> None:
        # TODO: asyncio.Future()로 내부 Future 생성
        self._future = asyncio.get_running_loop().create_future()

    async def wait(self) -> Any:
        """signal의 값 또는 예외를 비동기로 기다린다.

        Returns:
            signal의 값

        Raises:
            Exception: signal이 예외로 완료된 경우
        """
        # TODO: Future가 완료되었으면 결과 반환
        # TODO: 완료되지 않았으면 await로 대기
        # TODO: waiter 취소를 signal 취소로 전파하지 않도록 주의
        if self._future.done():
            return self._future.result()

        await asyncio.shield(self._future)

        return self._future.result()

    def set(self, value: Any) -> None:
        """signal을 값으로 완료한다.

        Raises:
            RuntimeError: 이미 완료된 경우
        """
        # TODO: 이미 완료되었으면 RuntimeError
        # TODO: set_result로 완료
        if self._future.done():
            raise RuntimeError
        self._future.set_result(value)

    def fail(self, exc: BaseException) -> None:
        """signal을 예외로 완료한다.

        Raises:
            RuntimeError: 이미 완료된 경우
        """
        # TODO: 이미 완료되었으면 RuntimeError
        # TODO: set_exception으로 완료
        if self._future.done():
            raise RuntimeError
        self._future.set_exception(exc)
