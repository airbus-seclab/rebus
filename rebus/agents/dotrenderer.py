import subprocess
from rebus.agent import Agent


@Agent.register
class DotRenderer(Agent):
    _name_ = "dotrenderer"
    _desc_ = "Render dot graphs as SVG files using graphviz"

    screen = None

    def selector_filter(self, selector):
        return selector.startswith("/graph/dot/")

    def process(self, desc, sender_id):
        dot = desc.value

        # ex. /graph/dot/qum/minhash%1234 is a qumgraph based on minhash
        # distances
        graphsrc, datatype = desc.selector.split('%')[0].split('/')[3:5]
        p = subprocess.Popen(["neato", "-Tsvg"],
                             stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        out, err = p.communicate(input=dot)

        d2 = desc.spawn_descriptor(
            "/graph/svg/%s/%s" % (graphsrc, datatype),
            out,
            self.name,
            label=desc.label + ' svg')
        self.push(d2)
