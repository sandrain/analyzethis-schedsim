#!/usr/bin/env python

import heapq

class TimeoutEvent:
    """descriptiont of an event
    """
    def __init__(self, name, timeout, handler):
        self.name = name
        self.timeout = timeout
        self.handler = handler

    def __lt__(self, other):
        return self.timeout < other.timeout

    def execute_handler(self):
        self.handler.handle_timeout(self)


class TimeoutEventHandler:
    """abstract class for components
    """
    def handle_timeout(self, event):
        pass


class EventSimulator:
    """discrete event simulation
    """
    def __init__(self):
        self.eq = []         # heap
        self.current = 0     # current time
        self.modules = []

    def register_module(self, module):
        self.modules += [ module ]

    def register_event(self, event):
        event.timeout += self.current
        heapq.heappush(self.eq, event)

    def prepare(self):
        for module in self.modules:
            self.register_event(TimeoutEvent('_init', 0, module))

    def run(self):
        while len(self.eq) > 0:
            e = heapq.heappop(self.eq)
            self.current = e.timeout
            events = [ e ]

            while len(self.eq) > 0:     # pop others with same timestamp
                next_event = heapq.heappop(self.eq)
                if next_event.timeout == self.current:
                    events += [ next_event ]
                else:
                    heapq.heappush(self.eq, next_event)
                    break

            for e in events:
                e.execute_handler()

        return self.current

