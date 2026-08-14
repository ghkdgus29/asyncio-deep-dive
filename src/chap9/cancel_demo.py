"""Standalone scheduler + Task.cancel() demo.

This is a separate copy of the io_scheduler.Scheduler with cancellation
support built in, kept independent from io_scheduler.py on purpose so that
file stays the plain "callback scheduler + tcp echo server" example.

Two focused demos:
1. Cancelling a task that's sleeping (a periodic "job" that runs forever
   until stopped).
2. Cancelling a task that's blocked on a socket recv() (a "read" that never
   arrives, e.g. a timeout).

Run: python src/chap9/cancel_demo.py
"""
import heapq
import socket
import time
from collections import deque
from select import select


class CancelledError(Exception):
    pass


class Scheduler:
    def __init__(self):
        self.ready = deque()  # Functions ready to execute
        self.sleeping = []  # Sleeping functions
        self.sequence = 0
        self._read_waiting = {}
        self._write_waiting = {}
        self.current: "Task | None" = None  # currently-running task, set by Task.__call__

    def call_soon(self, func):
        self.ready.append(func)

    def call_later(self, delay, func):
        self.sequence += 1
        deadline = time.time() + delay  # Expiration time
        # sequence breaks ties when deadlines match (heapq needs func to be unorderable-safe)
        heapq.heappush(self.sleeping, (deadline, self.sequence, func))

    def read_wait(self, fileno, func):
        self._read_waiting[fileno] = func

    def write_wait(self, fileno, func):
        self._write_waiting[fileno] = func

    def run(self):
        while self.ready or self.sleeping or self._read_waiting or self._write_waiting:
            if not self.ready:
                if self.sleeping:
                    deadline, _, func = self.sleeping[0]
                    timeout = deadline - time.time()
                    timeout = max(timeout, 0)
                else:
                    timeout = None  # Wait forever

                # select() with all-empty fd lists errors on Windows, so just
                # sleep when nothing is waiting on I/O (e.g. cancel_demo's
                # pure-sleep scenario).
                if self._read_waiting or self._write_waiting:
                    can_read, can_write, _ = select(
                        self._read_waiting, self._write_waiting, [], timeout
                    )
                else:
                    can_read, can_write = [], []
                    if timeout:
                        time.sleep(timeout)

                for fd in can_read:
                    task = self._read_waiting.pop(fd)
                    task.waiting_kind = None
                    self.ready.append(task)
                for fd in can_write:
                    task = self._write_waiting.pop(fd)
                    task.waiting_kind = None
                    self.ready.append(task)

                # Check for sleeping tasks
                now = time.time()
                while self.sleeping:
                    if now > self.sleeping[0][0]:
                        task = heapq.heappop(self.sleeping)[2]
                        # if cancel() already moved it elsewhere, this entry is stale
                        if task.waiting_kind == "sleep":
                            task.waiting_kind = None
                            self.ready.append(task)
                    else:
                        break

            while self.ready:
                func = self.ready.popleft()
                func()

    def new_task(self, coro):
        task = Task(coro)
        self.ready.append(task)
        return task  # handle for cancel()

    async def sleep(self, delay):
        self.current.waiting_kind = "sleep"  # lets cancel() recognize a stale heap entry
        self.call_later(delay, self.current)
        self.current = None
        await switch()

    async def recv(self, sock, maxbytes):
        self.current.waiting_kind = "read"
        self.current.waiting_key = sock  # lets cancel() find & remove this entry
        self.read_wait(sock, self.current)
        self.current = None
        await switch()
        return sock.recv(maxbytes)

    async def send(self, sock, data):
        self.current.waiting_kind = "write"
        self.current.waiting_key = sock
        self.write_wait(sock, self.current)
        self.current = None
        await switch()
        return sock.send(data)

    async def accept(self, sock):
        self.current.waiting_kind = "read"
        self.current.waiting_key = sock
        self.read_wait(sock, self.current)
        self.current = None
        await switch()
        return sock.accept()


class Task:
    def __init__(self, coro):
        self.coro = coro
        self.cancelled = False
        self.done = False  # coroutine has returned or raised; cancel() becomes a no-op
        self.waiting_kind: "str | None" = None  # "sleep" | "read" | "write", for cancel() to find it
        self.waiting_key = None  # sock, when waiting_kind is "read"/"write"

    def __call__(self):
        try:
            sched.current = self
            if self.cancelled:
                self.cancelled = False  # only raise once per cancel() call
                self.coro.throw(CancelledError)  # raised at the suspended await point
            else:
                self.coro.send(None)  # run until the next `await switch()`
            if sched.current:
                # still set to self => coroutine yielded without blocking on I/O/sleep,
                # so just reschedule it to run again next turn
                sched.ready.append(self)
            # else: current was cleared to None by sleep/recv/send/accept,
            # meaning the task is already queued in sleeping/_read_waiting/_write_waiting
        except (StopIteration, CancelledError):
            self.done = True

    def cancel(self):
        if self.cancelled or self.done:
            return  # cancel() already requested, or nothing left to cancel
        self.cancelled = True
        if self.waiting_kind == "read":
            sched._read_waiting.pop(self.waiting_key, None)
        elif self.waiting_kind == "write":
            sched._write_waiting.pop(self.waiting_key, None)
        # a "sleep" entry is left in the heap and discarded lazily in Scheduler.run()
        self.waiting_kind = None
        if self not in sched.ready:
            sched.ready.append(self)  # drive it so the throw() actually happens


class Awaitable:
    def __await__(self):
        yield  # bare yield: hands control back to Task.__call__ without a value


def switch():
    return Awaitable()


sched = Scheduler()

# ----------------


async def periodic_job():
    tick = 0
    try:
        while True:
            await sched.sleep(1)
            tick += 1
            print(f"[job] tick {tick}")
    except CancelledError:
        print("[job] cancelled, cleaning up")


async def watchdog(task, after):
    await sched.sleep(after)
    print(f"[watchdog] {after}s elapsed, cancelling job")
    task.cancel()


async def demo_cancel_sleep():
    print("=== Demo 1: cancelling a task blocked on sleep() ===")
    job = sched.new_task(periodic_job())
    sched.new_task(watchdog(job, after=3.5))


async def stalled_read(sock):
    print("[reader] waiting for data that will never arrive")
    try:
        await sched.recv(sock, 1024)
        print("[reader] got data (unexpected)")
    except CancelledError:
        print("[reader] cancelled, giving up on the read")


async def timeout_after(task, seconds):
    await sched.sleep(seconds)
    print(f"[timeout] {seconds}s elapsed, cancelling read")
    task.cancel()


_keepalive = []  # prevents `b` below from being GC'd (and closed) while unused


async def demo_cancel_io():
    print("=== Demo 2: cancelling a task blocked on recv() ===")
    a, b = socket.socketpair()  # `b` is kept open but nobody ever writes to it
    _keepalive.append(b)
    reader = sched.new_task(stalled_read(a))
    sched.new_task(timeout_after(reader, seconds=2))


async def main():
    await demo_cancel_sleep()
    # wait for demo 1 to fully finish before starting demo 2, so the output
    # of the two demos doesn't interleave
    await sched.sleep(4)
    await demo_cancel_io()


if __name__ == "__main__":
    sched.new_task(main())
    sched.run()
