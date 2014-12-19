from rebus.agent import Agent


@Agent.register
class Wait(Agent):
    _name_ = "wait"
    _desc_ = "Output any past or future descriptor whose selector starts "\
        "with provided string to stdout. Display first 500 characters only."

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

    def process(self, descriptor, sender_id):
        print repr(descriptor.value[:500])

    def run(self):
        self.wait_for += self.options.selectors
