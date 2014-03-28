from rebus.agent import Agent


@Agent.register
class Wait(Agent):
    _name_ = "wait"
    _desc_ = "Wait for some selectors and ouput their value to stdout"

    @classmethod
    def add_arguments(cls, subparser):
        subparser.add_argument("selectors", nargs="+",
                               help="Dump selector values on stdout")

    def init_agent(self):
        self.wait_for = []

    def selector_filter(self, selector):
        if self.wait_for:
            for s in self.wait_for:
                if selector.startswith(s):
                    return True
        return False

    def process(self, desc, sender_id):
        print repr(desc.value[:500])

    def run(self, options):
        self.wait_for += options.selectors
