import asyncio
import inspect


async def worker(ready: asyncio.Event) -> int:
    await ready.wait()
    return 42


async def main() -> None:
    ready = asyncio.Event()
    coroutine = worker(ready)  # CORO_CREATED

    print(inspect.getcoroutinestate(coroutine))

    task = asyncio.create_task(coroutine)
    await asyncio.sleep(0)

    print(inspect.getcoroutinestate(coroutine))  # CORO_SUSPENDED

    print(task.done())  # False
    ready.set()
    print(task.done())  # False
    result = await task
    print(task.done())  # True

    print(result)  # 42
    print(inspect.getcoroutinestate(coroutine))  # CORO_CLOSED

    print(task.done())  # True
    result2 = await task
    print(result2)  # 42


asyncio.run(main())
