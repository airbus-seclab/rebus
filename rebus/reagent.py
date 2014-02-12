import os
import hashlib
import cPickle
import logging

log = logging.getLogger("rebus.reagent")


class REdescriptor(object):
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



class REagent(object):
    def __init__(self, name, domain="default", transport=None):

        self.name = name
        self.domain = domain
        if transport is None:
            from rebus.transports.dbus_t import DBus
            transport = DBus()

        self.transport = transport
        self.transport.join(name, domain, callback=self.new_descriptor)
        self.agent_id = "%s-%s" % (name,self.transport.agent_id)
        self.transport.register_callback()

        self.init_agent()
        
    def get_past_descriptors(self, selector="/"):
        return self.transport.get_past_descriptors(selector)
        
    def push(self, descriptor):
        self.transport.push(descriptor.selector, descriptor.serialize())
        log.info("Pushed %r" % descriptor)
    def get(self, selector):
        return REdescriptor.unserialize(str(self.transport.get(selector)))
    def lock(self, selector):
        return self.transport.lock(selector)

    def new_descriptor(self, sender, domain, selector):
        if sender == self.agent_id: # Pushed by this agent
            return 
        log.debug("Received %s:%s" % (domain,selector))
        if domain != self.domain:
            return
        if self.selector_filter(selector):
            if self.lock(selector):
                desc = self.get(selector)
                if self.name in desc.agents:
                    return # already processed
                if self.descriptor_filter(desc):
                    log.info("START Processing %r" % desc)
                    self.process(desc)
                    log.info("END   processing %r" % desc)

    def mainloop(self):
        self.transport.mainloop()

    # These are the main methods that any agent would want
    # to overload
    def init_agent(self):
        pass
    def selector_filter(self, selector):
        return True
    def descriptor_filter(self, descriptor):
        return True
    def process(self, descriptor):
        raise NotImplemented("REagent.process()")




def run_filter_agent(agent_name, selector_filter, filter_func, new_selector):
    import logging
    class REfilteragent(REagent):
        def filter_selector(self, selector):
            return selector_filter(selector)
        def process(self, desc):
            v = filter_func(desc)
            d2 = desc.spawn_descriptor(new_selector, v, self.name)
            self.push(d2)

    logging.basicConfig(level=logging.INFO)
    agent = REfilteragent(agent_name)
    agent.mainloop()


