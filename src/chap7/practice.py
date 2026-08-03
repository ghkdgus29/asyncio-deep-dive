import asyncio
from collections.abc import Awaitable, Callable
from typing import Any


class WorkerPool:
    """비동기 worker pool."""

    def __init__(
        self,
        *,
        worker_count: int,
        queue_size: int,
        handler: Callable[[Any], Awaitable[Any]],
    ) -> None:
        # TODO: asyncio.Queue 생성 (maxsize=queue_size)
        # TODO: worker 수, handler 저장
        # TODO: worker Task들을 관리할 리스트 초기화
        self._queue = asyncio.Queue(queue_size)
        self._worker_count = worker_count
        self._handler = handler
        self._tasks = []
        self._close = False

    async def start(self) -> None:
        """worker를 시작한다."""

        # TODO: worker_count만큼 worker Task 생성
        # TODO: 각 worker는 queue에서 item을 꺼내 handler로 처리
        # TODO: queue.join()으로 작업 완료 대기
        async def worker():
            while True:
                item = await self._queue.get()
                await self._handler(item)
                self._queue.task_done()

        for _ in range(self._worker_count):
            task = asyncio.create_task(worker())
            self._tasks.append(task)

    async def submit(self, item: Any) -> None:
        """작업을 제출한다.

        Args:
            item: 처리할 작업

        Raises:
            RuntimeError: close 후 호출 시
        """
        # TODO: close 후에는 RuntimeError
        # TODO: queue.put(item)으로 작업 제출
        # TODO: queue가 가득 차면 대기 (backpressure)

        if self._close:
            raise RuntimeError

        await self._queue.put(item)

    async def join(self) -> None:
        """제출된 모든 작업의 처리가 끝날 때까지 기다린다."""
        # TODO: queue.join()으로 모든 작업 완료 대기

        await self._queue.join()

    async def close(self) -> None:
        """worker를 종료한다."""
        # TODO: close 플래그 설정
        # TODO: worker들이 종료될 수 있도록 poison pill 추가
        # TODO: 모든 worker Task 완료 대기

        self._close = True

        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
