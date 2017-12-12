import heapq
import threading
import time
import logging
log = logging.getLogger("rebus.tools.sched")

# python2-provided sched is not adequate:
# * not thread-safe, must use a lock (not true for python>=3.3)
# * does not trigger if a new task having a shorter timeout than all (if >0)
# currently pending tasks is added
# Notes regarding Timer objects:
# * a stopped (expired) timer can be cancel()led
# * obviously, a Timer can expire at any time, even when self._lock is held, so
#   checking whether it has expired is racy


class Sched(object):
    def __init__(self, injector=None):
        """
        :param injector: method which is called when a timer expires, from the
        sched thread. Args passed to add_action() will be passed as positional
        arguments.
        """
        self._injector = injector
        self._lock = threading.RLock()
        self._timer = None
        self._heap = []

    def add_action(self, time_offset, args):
        log.debug("add_action %d %s" % (time_offset, args))
        with self._lock:
            run_time = time.time()+time_offset
            if self._timer and run_time < self._heap[0][0]:
                # an expired timer can be cancelled
                self._timer.cancel()
                self._timer = None

            heapq.heappush(self._heap, (run_time, args))
            self._ensure_timer_started()

    def shutdown(self):
        with self._lock:
            if self._timer:
                self._timer.cancel()

    def _ensure_timer_started(self):
        with self._lock:
            if self._timer:
                return
            min_time = 30 + time.time()
            try:
                smallest = self._heap[0][0]
                min_time = min(min_time, smallest)
            except IndexError:
                # empty heap
                pass
            self._timer = threading.Timer(min_time-time.time(), self._trigger)
            self._timer.start()

    def _trigger(self):
        """
        Called by Timer object
        """
        with self._lock:
            self._timer = None
            run_time, args = heapq.heappop(self._heap)
            log.debug("trigger %d %s" % (run_time, args))
            self._injector(*args)
            if len(self._heap) > 0:
                self._ensure_timer_started()


if __name__ == '__main__':
    def f(arg0, arg1):
        print("%d %s" % (arg0, arg1))

    s = Sched(f)
    s.add_action(2, (0, "b"))
    s.add_action(1, (2, "a"))
    s.add_action(3, (4, "c"))
