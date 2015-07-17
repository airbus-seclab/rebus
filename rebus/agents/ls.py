import sys
from rebus.agent import Agent


@Agent.register
class Ls(Agent):
    _name_ = "ls"
    _desc_ = "List selector's children to stdout"
    _operationmodes_ = ('automatic', )

    @classmethod
    def add_arguments(cls, subparser):
        subparser.add_argument("--limit", nargs='?', type=int, default=0,
                               help="Max number of selectors to return")
        subparser.add_argument("selectors", nargs="*", default=[""],
                               help="Regex to match selectors, "
                               "results will be displayed on stdout")

    def run(self):
        for s in self.config['selectors']:
            sels = self.find(self.domain, s, self.config['limit'])
            if len(sels) > 0:
                for s in sels:
                    sys.stdout.write(s+"\n")
            else:
                self.log.warning("selector [%s:%s] not found",
                                 self.domain, s)
