import sys
from rebus.agent import Agent


@Agent.register
class Search(Agent):
    _name_ = "search"
    _desc_ = "Output a list of selectors for descriptors that match provided "\
             "domain, selector prefix and value regex"
    _operationmodes_ = ('automatic', )

    @classmethod
    def add_arguments(cls, subparser):
        subparser.add_argument("--domain", help="Descriptor domain",
                               default="default")
        subparser.add_argument("selector_prefix", nargs=1,
                               help="Selector prefix")
        subparser.add_argument("value_regex", nargs=1, help="Regex that the "
                               "value has to match (from its beginning)")

    def run(self):
        matches = self.bus.find_by_value(self, self.config['domain'],
                                         self.config['selector_prefix'][0],
                                         self.config['value_regex'][0])
        if len(matches) == 0:
            sys.stdout.write('No match found.\n')
        for match in matches:
            sys.stdout.write(str(match.selector))
            sys.stdout.write('\n')
