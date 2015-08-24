#!/usr/bin/env python

from itertools import *
from functools import reduce
import event
import host
import job

class Cluster(event.EventSimulator):
    def __init__(self, config):
        self.config = config
        self.servers = []
        self.clients = []
        event.EventSimulator.__init__(self)
        #self.ev = ev
        self.num_hosts = self.config.nodes
        self.tq = []
        print "Number of server nodes: %d" % (self.num_hosts)
        for i in range(self.num_hosts):
            myhost = host.Server (self, i, config)
            self.servers.append (myhost)
        for i in range(len(self.servers)):
            myhost = self.servers[i]
            print myhost.get_state()

        # We currently support only one client 
        self.num_clients = 1
        print "Number of clients: %d" % (self.num_clients)
        for i in range(self.num_clients):
            myclient = host.Client (i, config)
            self.clients.append (myclient)
        for i in range(len(self.clients)):
            myclient = self.clients[i]
            print myclient.get_state()

    # XXX when we will have the meta-scheduler fully functional, this code
    # should be able to move to the client class
    def prepare_workflow(self, workflow):
        js = workflow.xmlToJson ()
        # XXX For now, we tell the first server to populate the files
        # XXX Call libanalyzethis here and use the meta-scheduler
        self.servers[0].afs.job = job.ActiveJob(js)
        self.servers[0].afs.populate_files()
        for i in (range(self.num_hosts - 1)):
            self.servers[i+1].afs.job = None
            self.servers[i+1].afs.populate_files()

    def submit_workflow(self, workflow):
        # XXX We assume we have only client at the moment

        # We prepare the workflow
        self.prepare_workflow(workflow)

        # We submit the workflow
        # XXX For now, we tell the first server to execute the entire workflow,
        # assuming the static placement was the no-op policy (i.e., no
        # actual placement
        self.servers[0].afs.submit_workflow (workflow)
        for i in (range(self.num_hosts - 1)):
            self.servers[i+1].afs.submit_workflow (None)

    def report(self):
        self.servers[0].report()

    def handle_prepared_tasks(self):
        print "%s: not yet implemented" % self.__name__
