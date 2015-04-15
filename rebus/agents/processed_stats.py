import sys
from rebus.agent import Agent


@Agent.register
class ProcessedStats(Agent):
    _name_ = "processed_stats"
    _desc_ = "Display current processing stats for the given domain then exit"
    _operationmodes_ = ('automatic', )

    @classmethod
    def add_arguments(cls, subparser):
        subparser.add_argument("--domain", help="Domain",
                               default="default")

    def run(self):
        stats, total = self.processed_stats(self.config['domain'])
        for agentname, number in stats:
            sys.stdout.write("%s: %d/%d\n" % (agentname, number, total))
