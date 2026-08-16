"""Same two cancellation demos as cancel_demo.py, but with a real Future
abstraction instead of manual waiting_kind/waiting_key bookkeeping.

Every wait point (sleep, recv, send, accept) now creates one Future and
awaits it. Task no longer needs to know *what* it's waiting for -- it just
tracks the single Future it's currently blocked on (self._fut_waiter) and
cancels that one object. This mirrors real asyncio.Task.cancel(), which is
just:

    if self._fut_waiter is not None:
        self._fut_waiter.cancel()

regardless of whether that Future represents a timer, a socket read, a
queue get, or anything else -- compare Task.cancel() here to the
if/elif "read"/"write"/"sleep" dispatch in cancel_demo.py's version.

Run: python src/chap9/cancel_demo_future.py
"""

import heapq
import socket
import time
from collections import deque
from select import select


class CancelledError(Exception):
    pass


class Future:
    """A box for a value that becomes available later.

    Every kind of "waiting" (sleep, socket I/O, ...) creates one of these
    and awaits it. That's the whole point of the abstraction: the Task
    driving the coroutine never needs to know what's behind the Future.
    """

    def __init__(self):
        self._done = False
        self._cancelled = False
        self._result = None
        self._callbacks = []

    def done(self):
        return self._done

    def cancelled(self):
        return self._cancelled

    def set_result(self, result):
        if self._done:
            return  # already resolved (e.g. cancelled first) -- ignore, like
            # asyncio's _set_result_unless_cancelled
        self._result = result
        self._done = True
        self._run_callbacks()

    def cancel(self):
        if self._done:
            return False
        self._cancelled = True
        self._done = True
        self._run_callbacks()
        return True

    def add_done_callback(self, cb):
        if self._done:
            sched.call_soon(lambda: cb(self))
        else:
            self._callbacks.append(cb)

    def _run_callbacks(self):
        callbacks, self._callbacks = self._callbacks, []
        for cb in callbacks:
            sched.call_soon(lambda cb=cb: cb(self))

    def result(self):
        if self._cancelled:
            raise CancelledError()
        return self._result

    def __await__(self):
        if not self._done:
            yield self  # hand the Future itself back to Task.__call__
        return self.result()


class Scheduler:
    def __init__(self):
        self.ready = deque()  # Functions ready to execute
        self.sleeping = []  # (deadline, seq, callback)
        self.sequence = 0
        self._read_waiting = {}  # fileno -> callback
        self._write_waiting = {}  # fileno -> callback

    def call_soon(self, func):
        self.ready.append(func)

    def call_later(self, delay, func):
        self.sequence += 1
        deadline = time.time() + delay
        heapq.heappush(self.sleeping, (deadline, self.sequence, func))

    def read_wait(self, fileno, func):
        self._read_waiting[fileno] = func

    def write_wait(self, fileno, func):
        self._write_waiting[fileno] = func

    def run(self):
        while self.ready or self.sleeping or self._read_waiting or self._write_waiting:
            if not self.ready:
                if self.sleeping:
                    deadline, _, _ = self.sleeping[0]
                    timeout = max(deadline - time.time(), 0)
                else:
                    timeout = None  # wait forever

                can_read, can_write, _ = select(
                    self._read_waiting, self._write_waiting, [], timeout
                )

                # Fire the registered callback directly -- for recv/send/accept
                # this callback is `fut.set_result(...)`, which is a no-op if
                # the Future was already cancelled in the meantime.
                for fd in can_read:
                    self._read_waiting.pop(fd)()
                for fd in can_write:
                    self._write_waiting.pop(fd)()

                now = time.time()
                while self.sleeping:
                    if now > self.sleeping[0][0]:
                        heapq.heappop(self.sleeping)[2]()
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
        fut = Future()
        self.call_later(delay, lambda: fut.set_result(None))
        await fut

    async def recv(self, sock, maxbytes):
        fut = Future()
        self.read_wait(sock, lambda: fut.set_result(None))
        # if cancelled before the socket ever became readable, drop the
        # stale registration so select() stops watching it
        fut.add_done_callback(lambda f: self._read_waiting.pop(sock, None))
        await fut
        return sock.recv(maxbytes)

    async def send(self, sock, data):
        fut = Future()
        self.write_wait(sock, lambda: fut.set_result(None))
        fut.add_done_callback(lambda f: self._write_waiting.pop(sock, None))
        await fut
        return sock.send(data)

    async def accept(self, sock):
        fut = Future()
        self.read_wait(sock, lambda: fut.set_result(None))
        fut.add_done_callback(lambda f: self._read_waiting.pop(sock, None))
        await fut
        return sock.accept()


class Task:
    def __init__(self, coro):
        self.coro = coro
        self.done = False
        self._fut_waiter = None  # the *one* Future this task is blocked on
        self._must_cancel = False

    def __call__(self, exc=None):
        self._fut_waiter = None
        try:
            if self._must_cancel and exc is None:
                exc = CancelledError()
                self._must_cancel = False
            if exc is not None:
                result = self.coro.throw(exc)
            else:
                result = self.coro.send(None)
        except StopIteration, CancelledError:
            self.done = True
            return

        if isinstance(result, Future):
            self._fut_waiter = result
            result.add_done_callback(self._wakeup)
        else:
            # bare yield (shouldn't really happen anymore, everything awaits
            # a Future now) -- just run again next turn
            sched.ready.append(self)

    def _wakeup(self, future):
        if future.cancelled():
            self(exc=CancelledError())
        else:
            self()

    def cancel(self):
        if self.done:
            return False  # already finished, nothing to cancel
        if self._fut_waiter is not None:
            # <-- the whole point: one line, no waiting_kind dispatch
            return self._fut_waiter.cancel()  # blocked on a Future: cancel that
        self._must_cancel = True  # not waiting on anything yet: flag for next __call__
        if self not in sched.ready:
            sched.ready.append(self)
        return True


sched = Scheduler()


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
    a, b = socket.socketpair()  # `b` is kept open but nobody ever writes to it
    _keepalive.append(b)
    reader = sched.new_task(stalled_read(a))
    sched.new_task(timeout_after(reader, seconds=2))


async def main():
    await demo_cancel_io()


if __name__ == "__main__":
    sched.new_task(main())
    sched.run()
