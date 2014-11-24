import sys
from rebus.agent import Agent
import re

@Agent.register
class Return(Agent):
    _name_ = "return"
    _desc_ = "Output any past or future descriptor whose selector matches "\
        "provided regex to stdout"

    @classmethod
    def add_arguments(cls, subparser):
        subparser.add_argument(
            "selectors", nargs="+", 
            help="Dump selector values on stdout. Selectors can be regexes")
        subparser.add_argument(
            "--raw", action="store_true", help="Raw output")

    def selector_filter(self, selector):
        for selregex in self.options.selectors:
            if re.search(selregex, selector):
                return True
        return False
    
    def process(self, descriptor, sender_id):
        if not self.options.raw:
            print "---------------------------"
            print "selector = %s" % descriptor.selector
            print "label = %s" % descriptor.label
            print "UUID = %s" % descriptor.uuid
        print descriptor.value
