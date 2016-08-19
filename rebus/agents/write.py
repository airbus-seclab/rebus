import os
import re
from rebus.agent import Agent


@Agent.register
class Return(Agent):
    _name_ = "write"
    _desc_ = "Write values of descriptors matching a regex into files"
    _operationmodes_ = ('automatic', )

    @classmethod
    def add_arguments(cls, subparser):
        subparser.add_argument(
            "selectors", nargs="+",
            help="Dump selector values on stdout. Selectors can be regexes")
        subparser.add_argument("--flat", action="store_true",
                               help="Does not create one folder per UUID")
        subparser.add_argument("--target-dir", default=".",
                               help="Target folder")

    def selector_filter(self, selector):
        for selregex in self.config['selectors']:
            if re.search(selregex, selector):
                return True
        return False

    def process(self, descriptor, sender_id):
        target = self.config['target_dir']
        if not self.config['flat']:
            target = os.path.join(target, descriptor.uuid)
        if not os.path.exists(target):
            os.makedirs(target)
        target = os.path.join(target, descriptor.label)
        nb = ""
        i = 0
        while os.path.exists(target + nb):
            i += 1
            nb = "."+str(i)
        open(target+nb, "w").write(str(descriptor.value))
