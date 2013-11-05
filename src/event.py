#!/usr/bin/env python

import heapq

class TimeoutEvent:
    """descriptiont of an event
    """
    def __init__(self, name, timeout, handler):
        self.name = name
        self.timeout = timeout
        self.handler = handler

    def timeout(self):
        self.handler.timeout(self)


class TimeoutEventHandler:
    """abstract class for components
    """
    def timeout(self, event):
        pass


class EventSimulator:
    """discrete event simulation
    """
    def __init__(self):
        self.eq = []         # heap
        self.current = 0     # current time

    def register_event(self, event):
        event.timeout += self.current
        entry = [ event.timeout, event ]
        heapq.heappush(self.eq, entry)

    def simloop(self):
        while len(self.eq) > 0:
            events = [ heapq.heappop(self.eq) ]
            self.current = event.timeout

            while True:
                next_event = heapq.heappop(self.eq)
                if next_event.timeout == self.current:
                    events += [ next_event ]
                else:
                    heapq.heappush(self.eq, next_event)
                    break

            for ev in events:
                ev[1].timeout()

        return self.current


