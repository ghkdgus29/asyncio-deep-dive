import asyncio
from collections.abc import Callable
from typing import Any


async def fetch_user_profile(
    user_id: int,
    *,
    fetch_user: Callable[[int], Any],
    fetch_orders: Callable[[int], Any],
    fetch_preferences: Callable[[int], Any],
    timeout: float,
) -> dict[str, Any]:
    """사용자, 주문, 환경설정을 동시에 조회한다.

    Args:
        user_id: 사용자 ID
        fetch_user: 사용자 정보를 가져오는 async 함수
        fetch_orders: 주문 정보를 가져오는 async 함수
        fetch_preferences: 환경설정 정보를 가져오는 async 함수
        timeout: 전체 timeout (초)

    Returns:
        {"user": ..., "orders": ..., "preferences": ...}

    Raises:
        ExceptionGroup: 하나 이상의 fetch가 실패한 경우
        TimeoutError: timeout 초과 시
    """
    async with asyncio.timeout(timeout):
        async with asyncio.TaskGroup() as tg:
            user_task = tg.create_task(fetch_user(user_id))
            order_task = tg.create_task(fetch_orders(user_id))
            prefer_task = tg.create_task(fetch_preferences(user_id))

        return {
            "user": user_task.result(),
            "orders": order_task.result(),
            "preferences": prefer_task.result(),
        }
