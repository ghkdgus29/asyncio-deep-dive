from collections import deque

from src.chap9.scheduler import scheduler


class QueueClosed(Exception):
    def __init__(self):
        super().__init__("Queue is closed")


class Result:
    def __init__(self, value=None, exc=None):
        self.value = value
        self.exc = exc

    def result(self):
        if self.exc:
            raise self.exc
        else:
            return self.value


class AsyncQueue:
    def __init__(self):
        self.items = deque()
        self.waiting = deque()  # All getters waiting for data
        self._closed = False

    def close(self):
        self._closed = True
        if self.waiting and not self.items:
            for func in self.waiting:
                scheduler.call_soon(func)

    def put(self, item):
        if self._closed:
            raise QueueClosed()

        self.items.append(item)
        if self.waiting:
            func = self.waiting.popleft()
            scheduler.call_soon(func)

    def get(self, callback):
        # Wait until an item is available. Then return it.
        if self.items:
            callback(Result(value=self.items.popleft()))
        else:
            if self._closed:
                callback(Result(exc=QueueClosed()))
            else:
                self.waiting.append(lambda: self.get(callback))


def producer(q: AsyncQueue, count: int):
    def _run(n):
        if n < count:
            print("Producing", n)
            q.put(n)
            scheduler.call_later(1, lambda: _run(n + 1))
        else:
            print("Producer done")
            q.close()

    _run(0)


def consumer(q: AsyncQueue):
    def _consume(result):
        try:
            item = result.result()
            print("Consuming", item)
            scheduler.call_soon(lambda: consumer(q))
        except QueueClosed:
            print("Consumer done")

    q.get(callback=_consume)


q = AsyncQueue()
scheduler.call_soon(lambda: producer(q, 10))
scheduler.call_soon(lambda: consumer(q))
scheduler.run()
