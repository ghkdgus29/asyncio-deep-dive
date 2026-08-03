import asyncio
from typing import Protocol


class AsyncResource(Protocol):
    async def open(self) -> None: ...
    async def read(self) -> str: ...
    async def close(self) -> None: ...


async def process_with_resource(
    resource: AsyncResource,
    *,
    timeout: float,
) -> str:
    """리소스를 열고, 읽고, 닫는다.

    Args:
        resource: 비동기 open, read, close를 제공하는 리소스
        timeout: 전체 작업 timeout (초)

    Returns:
        read 결과

    Raises:
        TimeoutError: timeout 초과 시
        CancelledError: 외부 취소 시
        Exception: open/read/close 실패 시
    """
    # TODO: open은 실패 시 close를 호출하면 안 된다 — open이 try/finally 안에 있어야 할지 생각해볼 것
    # TODO: 성공/실패/timeout/취소 "모든" 경로에서 close가 한 번만 불리게 하려면 어떤 구문이 필요할까
    # TODO: 전체에 timeout을 걸 수 있는 asyncio 함수를 찾아볼 것
    # TODO: 취소를 못 삼키게 하려면, 오히려 뭘 하지 말아야 할지 생각해볼 것

    await resource.open()
    try:
        async with asyncio.timeout(timeout):
            return await resource.read()
    finally:
        await resource.close()
