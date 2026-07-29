import inspect


async def compute() -> int:
    print("body started")
    return 42


coroutine = compute()

print(type(coroutine))
print(inspect.iscoroutine(coroutine))
print(inspect.getcoroutinestate(coroutine))
print(coroutine)

coroutine.close()
