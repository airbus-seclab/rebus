import sys
import re
from rebus.agent import Agent


@Agent.register
class Ls(Agent):
    _name_ = "ls"
    _desc_ = "List selector's children to stdout"

    @classmethod
    def add_arguments(cls, subparser):
        subparser.add_argument("selectors", nargs="+",
                               help="Dump selector values on stdout")

    def run(self, options):
        for s in options.selectors:
            sels = self.find(self.domain, s, limit=10)
            if len(sels) > 0:
                for s in sels:
                    sys.stdout.write(s+"\n")
            else:
                self.log.warning("selector [%s:%s] not found", options.domain,
                                 s)
