#!/usr/bin/env python

from itertools import *
from functools import reduce
import event
import host
import job

class Cluster():
    def __init__(self, config):
        self.config = config
        self.servers = []
        self.clients = []
        #self.ev = ev
        self.num_hosts = self.config.nodes
        print "Number of server nodes: %d" % (self.num_hosts)
        for i in range(self.num_hosts):
            myhost = host.Server (i, config)
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

    def submit_workflow(self, workflow):
        # XXX We assume we have only client at the moment

        # We prepare the workflow
        self.prepare_workflow(workflow)

        # We submit the workflow
        # XXX For now, we tell the first server to execute the entire workflow,
        # assuming the static placement was the no-op policy (i.e., no
        # actual placement
        self.servers[0].afs.submit_workflow (workflow)

    def prepare(self):
        self.servers[0].prepare()

    def run(self):
        return self.servers[0].run()

    def report(self):
        self.servers[0].report()
