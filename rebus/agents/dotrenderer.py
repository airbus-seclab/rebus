import subprocess
from rebus.agent import Agent


@Agent.register
class DotRenderer(Agent):
    _name_ = "dotrenderer"
    _desc_ = "Render dot graphs as SVG files using graphviz"
    _operationmodes_ = ('automatic', 'interactive')

    screen = None

    def selector_filter(self, selector):
        return selector.startswith("/graph/dot/")

    def process(self, descriptor, sender_id):
        dot = descriptor.value

        # ex. /graph/dot/qum/minhash%1234 is a qumgraph based on minhash
        # distances
        graphsrc, datatype = descriptor.selector.split('%')[0].split('/')[3:5]
        p = subprocess.Popen(["neato", "-Tsvg"],
                             stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        out, err = p.communicate(input=dot)

        d2 = descriptor.spawn_descriptor(
            "/graph/svg/%s/%s" % (graphsrc, datatype),
            out,
            self.name,
            label=descriptor.label + ' svg')
        self.push(d2)
