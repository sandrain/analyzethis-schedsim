#!/usr/bin/env python

import heapq

class TimeoutEvent:
    """descriptiont of an event
    """
    def __init__(self, name, timeout, handler):
        self.name = name
        self.registered = 0.0
        self.timeout = timeout
        self.handler = handler
        self.context = None
        self.description = None
        self.disposable = False

    def __lt__(self, other):
        return self.timeout < other.timeout

    def set_disposable(self):
        self.disposable = True

    def set_timeout(self, timeout):
        self.timeout = timeout

    def set_context(self, context):
        self.context = context

    def get_context(self):
        return self.context

    def set_description(self, desc):
        self.description = desc

    def get_description(self):
        return self.description

    def execute_handler(self):
        self.handler.handle_timeout(self)


class TimeoutEventHandler:
    """abstract class for components
    """
    def get_name(self):
        pass

    def handle_timeout(self, event):
        pass

    def report(self):
        pass


class EventSimulator:
    """discrete event simulation
    """
    def __init__(self):
        self.eq = []         # heap
        self.current = 0.0   # current time
        self.modules = []
        self.terminated = False

    def register_module(self, module):
        self.modules += [ module ]

    def register_event(self, event):
        event.registered = self.current
        event.timeout += self.current
        heapq.heappush(self.eq, event)

    def prepare(self):
        for module in self.modules:
            self.register_event(TimeoutEvent('init', 0, module))

    def terminate(self):
        if not self.terminated:
            self.terminated = True
            for module in self.modules:
                self.register_event(TimeoutEvent('exit', 0, module))
            li = [ x for x in self.eq if x.disposable ]
            for e in self.eq[:]:
                if e in li:
                    self.eq.remove(e)

    def now(self):
        return self.current

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
                if not self.terminated:
                    e.execute_handler()

        return self.current

    def report(self):
        pass
        """
        for module in self.modules:
            print('\n=====', module.get_name(), '=====')
            module.report()
            """


