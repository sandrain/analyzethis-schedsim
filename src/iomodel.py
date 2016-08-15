#!/usr/bin/env python

class EmulatorIOModel:
    def __init__(self, config):
        self.config = config
        # The cost for transfering a small file (this cost cannot be hidden
        # with the actual transfer cost. This should be configurable
        self.SMALLFILEOVERHEAD = 0

    def get_transfer_cost(self, f):
        if f.size > 1000000:
            transfer_time = 2.0 * (0.3 + float(f.size) * 1.02 / self.config.netbw)
        else:
            transfer_time = self.SMALLFILEOVERHEAD
        return transfer_time

class DefaultIOModel:
    def __init__(self, config):
        self.config = config

    def get_transfer_cost(self, f):
        return 2.0 * (0.3 + float(f.size) * 1.02 / self.config.netbw)

class IOModel:
    def __init__(self, config, module_name):
        self.module = None
        self.config = config

        if module_name == "Emulator":
            self.module = EmulatorIOModel(self.config)
        else:
            self.module = DefaultIOModel(self.config)

    def get_transfer_cost(self, afile):
        return self.module.get_transfer_cost(afile)
