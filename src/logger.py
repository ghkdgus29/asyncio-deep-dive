import asyncio
import time

STARTED_AT = time.perf_counter()


def log(message: str) -> None:
    elapsed = time.perf_counter() - STARTED_AT

    try:
        task = asyncio.current_task()
    except RuntimeError:
        task = None

    task_name = task.get_name() if task else "sync"
    print(f"{elapsed:7.3f}s [{task_name:<16}] {message}")
