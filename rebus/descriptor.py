import os
import hashlib
import cPickle
import logging

log = logging.getLogger("rebus.descriptor")

class Descriptor(object):
    def __init__(self, label, selector, value, domain = "default",
                 agents=None, precursors=None):
        self.label = label
        self.agents = agents if agents else []
        self.precursors = precursors if precursors is not None else []
        p = selector.rfind("%")
        if p >= 0:
            self.hash = selector[p+1:]
        else:
            if self.agents and self.precursors:
                v = self.agents[0]+self.precursors[0]
            else:
                v = value if type(value) is str else cPickle.dumps(value)
            self.hash = hashlib.sha256(v).hexdigest()
            selector = os.path.join(selector, "%"+self.hash)
        self.selector = selector
        self.value = value
        self.domain = domain

    def spawn_descriptor(self, selector, value, agent):
        desc = self.__class__(self.label, selector, value, self.domain,
                              agents = [agent]+self.agents,
                              precursors = [self.selector]+self.precursors)
        return desc

    def serialize(self):
        return cPickle.dumps(
            { k:v for k,v in self.__dict__.iteritems()
              if k in ["label", "selector", "value",
                       "domain", "agents", "precursors"] } )
    @classmethod
    def unserialize(cls, s):
        return cls(**cPickle.loads(s))

    def __repr__(self):
        v = repr(self.value)
        if len(v) > 30:
            v = "[%i][%s...]" % (len(v), v[:22])
        return "%s:%s(%s)=%s" % (self.domain, self.selector, self.label, v)


