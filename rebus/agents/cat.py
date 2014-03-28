import sys
from rebus.agent import Agent


@Agent.register
class Cat(Agent):
    _name_ = "cat"
    _desc_ = "Dump a selector's value from the bus to stdout"

    @classmethod
    def add_arguments(cls, subparser):
        subparser.add_argument("selectors", nargs="+",
                               help="Dump selector values on stdout")
        subparser.add_argument("selectors", nargs="+",
                               help="Dump selector values on stdout")

    def run(self, options):
        for s in options.selectors:
            desc = self.get(options.domain, s)
            if desc:
                sys.stdout.write(desc.value)
            else:
                self.log.warning("selector [%s:%s] not found", options.domain,
                                 s)
