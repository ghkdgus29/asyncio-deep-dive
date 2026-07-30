from __future__ import annotations

import asyncio
import inspect
from collections.abc import Coroutine, Generator
from typing import Any

from src.logger import log


class NextLoop:
    def __init__(self, *, yield_control: bool) -> None:
        self.yield_control = yield_control

    def __await__(self) -> Generator[None]:
        if self.yield_control:
            log("NextLoop: before yield")
            yield
            log("NextLoop: after yield")
        else:
            log("NextLoop: skip yield")

        return None


async def worker(name: str, *, yield_control: bool) -> None:
    log(f"{name}: before await")
    await NextLoop(yield_control=yield_control)
    log(f"{name}: after await")


def print_states(
    label: str,
    *coroutines: Coroutine[Any, Any, Any],
) -> None:
    states = [inspect.getcoroutinestate(coroutine) for coroutine in coroutines]
    print(f"{label}: {states}")


async def run_case(*, yield_control: bool) -> None:
    print(f"\n=== yield_control={yield_control} ===")

    worker_a = worker("A", yield_control=yield_control)
    worker_b = worker("B", yield_control=yield_control)
    print_states("after creation", worker_a, worker_b)

    task_a = asyncio.create_task(worker_a, name="worker-A")
    task_b = asyncio.create_task(worker_b, name="worker-B")

    await asyncio.sleep(0)
    print_states("after one loop turn", worker_a, worker_b)

    await asyncio.gather(task_a, task_b)
    print_states("after gather", worker_a, worker_b)


async def main() -> None:
    await run_case(yield_control=True)
    await run_case(yield_control=False)


asyncio.run(main())
