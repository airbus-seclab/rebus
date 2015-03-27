import sys
from rebus.agent import Agent


@Agent.register
class Cat(Agent):
    _name_ = "cat"
    _desc_ = "Dump a selector's value from the bus to stdout"
    _operationmodes_ = ('automatic', )

    @classmethod
    def add_arguments(cls, subparser):
        subparser.add_argument(
            "selectors", nargs="+", help="Find up to 3 existing descriptor "
            "whose selector matches provided regex, and dump their values.")
        subparser.add_argument('--raw', help="Output descriptors' raw values",
                               action='store_true')

    def run(self):
        for selregex in self.config['selectors']:
            sels = self.find(self.domain, selregex, 3)
            if len(sels) > 0:
                for s in sels:
                    desc = self.get(self.domain, s)
                    if desc:
                        if self.config['raw']:
                            sys.stdout.write(str(desc.value))
                        else:
                            sys.stdout.write(desc.selector+":\n")
                            sys.stdout.write(str(desc.value))
                            sys.stdout.write("\n")
                    else:
                        self.log.warning("selector [%s:%s] not found",
                                         self.domain, s)
