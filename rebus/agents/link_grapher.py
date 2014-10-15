import sys
import time
from itertools import chain
from rebus.agent import Agent
from rebus.descriptor import Descriptor


@Agent.register
class LinkGrapher(Agent):
    _name_ = "link_grapher"
    _desc_ = "Create a dot graph from links between analysis"

    @classmethod
    def add_arguments(cls, subparser):
        subparser.add_argument("--limit", nargs='?', type=int, default=0,
                               help="Max number of selectors to return")
        subparser.add_argument("selectors", nargs="*", default = ["/link/.*"],
                               help="Regex to match /link/ selectors,\
                                     results will be displayed on stdout")

    def run(self, options):

        start = time.time()
        
        def ensure_link(x):
            if x.startswith("/link/"):
                return x
            if x.startswith("/"):
                return "/link"+x
            return "/link/"+x
        
        sels = chain(*[map(str,self.find(self.domain, ensure_link(s), options.limit)) 
                       for s in options.selectors])


        class Component(object):
            def __init__(self, linktype):
                self.linktype = linktype
                self.nodes = set()
            def add(self, v):
                self.nodes.add(v)



        links = {}
        labels = {}

        def nodenamer(fmt="node%i"):
            i = 0
            while True:
                yield fmt % i
                i += 1


        for s in sels:
            link = self.get(self.domain, s)
            uu1,uu2 = link.uuid, link.value["otherUUID"]
            linktype = link.value["linktype"]
            labels[uu1] = link.label
            labels[uu2] = link.value["otherlabel"]

            component = links.get((uu1,linktype)) or links.get((uu2,linktype))
            if not component:
                component = Component(linktype)
            component.add(uu1)
            component.add(uu2)
            links[uu1,linktype] = links[uu2,linktype] = component
        
        ltname = nodenamer()

        dot = [ 'graph "links" {' ]

        for n,l in labels.iteritems():
            dot.append('\t"%s" [ label="%s", fontsize=10, fillcolor="#ffccdd", style=filled, shape=note, href="/analysis/%s/%s"];' % (n,l,self.domain,n))

        dot.append("")

        for comp in set(links.values()):
            compname = ltname.next()
            dot.append('\t"%s" [ label="%s", fontsize=8, fillcolor="#ccddff", style=filled, shape=oval];' % (compname, comp.linktype))
            for elt in comp.nodes:
                dot.append('\t"%s" -- "%s" [ len=2 ];' % (compname, elt))
            dot.append("")

        dot.append("}")
        done = time.time()

        desc = Descriptor(label="linkgraph", 
                          selector="/graph/dot/linkgraph",
                          value="\n".join(dot),
                          domain=self.domain,
                          agent=self._name_,
                          processing_time=done-start)

        self.push(desc)
