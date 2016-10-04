from rebus.agent import Agent


@Agent.register
class Ls(Agent):
    _name_ = "ls_uuid"
    _desc_ = "List existing UUIDs and their labels"
    _operationmodes_ = ('automatic', )

    @classmethod
    def add_arguments(cls, subparser):
        subparser.add_argument("--domain", help="Descriptor domain",
                               default="default")

    def run(self):
        domain = self.config['domain']
        uuidict = self.bus.list_uuids(self, domain)
        for uuid, label in uuidict.items():
            print "%s %s" % (uuid, label)
