import sys
from rebus.agent import Agent


@Agent.register
class Cat(Agent):
    _name_ = "cat"
    _desc_ = "Dump a selector's value from the bus to stdout"

    @classmethod
    def add_arguments(cls, subparser):
        subparser.add_argument("selectors", nargs="+",
                               help="Dump selector values on stdout. Selectors can be regexes")

    def run(self, options):
        for selregex in options.selectors:
            sels = self.find(self.domain, selregex, 3)
            print selregex, sels
            if len(sels) > 0:
                for s in sels:
                    desc = self.get(options.domain, s)
                    if desc:
                        sys.stdout.write(desc.selector+":\n")
                        sys.stdout.write(str(desc.value))
                        sys.stdout.write("\n")
                    else:
                        self.log.warning("selector [%s:%s] not found", options.domain, s)
