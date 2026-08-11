import heapq
import time
from collections import deque


class Scheduler:
    def __init__(self):
        self.ready = deque()  # Functions ready to execute
        self.sleeping = []
        self.sequence = 0

    def call_soon(self, func):
        self.ready.append(func)

    def call_later(self, delay, func):
        self.sequence += 1
        deadline = time.time() + delay
        heapq.heappush(self.sleeping, (deadline, self.sequence, func))

    def run(self):
        while self.ready or self.sleeping:
            if not self.ready:
                # Find the nearest deadline
                deadline, _, func = heapq.heappop(self.sleeping)
                delta = deadline - time.time()
                if delta > 0:
                    time.sleep(delta)
                self.ready.append(func)

            while self.ready:
                func = self.ready.popleft()
                func()


scheduler = Scheduler()


def countdown(n):
    if n > 0:
        print("Down", n)
        # time.sleep(1)
        scheduler.call_later(4, lambda: countdown(n - 1))


def countup(stop, x=0):
    if x < stop:
        print("Up", x)
        # time.sleep(1)
        scheduler.call_later(1, lambda: countup(stop, x + 1))


# scheduler.call_soon(lambda: countdown(5))
# scheduler.call_soon(lambda: countup(20))
# scheduler.run()
