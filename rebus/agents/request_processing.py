#! /usr/bin/env python
from rebus.agent import Agent


@Agent.register
class RequestProcessing(Agent):
    _name_ = "request_processing"
    _desc_ = "Request processing by agent operating in interactive mode"
    _operationmodes_ = ('automatic', )

    @classmethod
    def add_arguments(cls, subparser):
        subparser.add_argument(
            "domain", help="domain where the descriptor belongs")
        subparser.add_argument(
            "selector", help="full selector (ex. /binary/pe/%abcd1234...")
        subparser.add_argument(
            "targets", nargs="+",
            help="names of agents that should honor this processing request")

    def run(self):
        print "args:", self.config
        self.bus.request_processing(self.id, self.config['domain'],
                                    self.config['selector'],
                                    self.config['targets'])
