import sys
import re
from rebus.agent import Agent


@Agent.register
class LsIdle(Agent):
    _name_ = "ls_idle"
    _desc_ = "List selectors matching regex when the bus is idle"
    _operationmodes_ = ('idle', )

    @classmethod
    def add_arguments(cls, subparser):
        subparser.add_argument("--limit", nargs='?', type=int, default=0,
                               help="Max number of selectors to return")
        subparser.add_argument("selectors", nargs="?", default=".*",
                               help="Regex to match selectors, "
                               "results will be displayed on stdout")
    def init_agent(self):
        self.selectors_regex = re.compile(self.config['selectors'])

    def selector_filter(self, selector):
        return self.selectors_regex.match(selector)

    def process(self, descriptor, sender_id):
        sys.stdout.write(descriptor.selector+"\n")

