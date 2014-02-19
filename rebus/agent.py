from rebus.tools.registry import Registry
import logging


log = logging.getLogger("rebus.agent")

class AgentRegistry(Registry):
    pass


class AgentLogger(logging.LoggerAdapter):
    def process(self, msg, kargs):
        return "[%s] %s" % (self.extra["agent_id"], msg), kargs

class Agent(object):
    _name_ = "Agent"
    _desc_ = "N/A"

    @staticmethod
    def register(f):
        return AgentRegistry.register_ref(f, key="_name_")

    def __init__(self, bus, name=None, domain="default"):
        self.name = name if name else self._name_
        self.domain = domain
        self.bus = bus
        self.id = self.bus.join(self.name, domain, callback=self.on_new_descriptor)
        self.log = AgentLogger(log, dict(agent_id=self.id))
        self.log.info("Agent {0.name} registered on bus {1._name_} with id {0.id}".format(self, self.bus))

        self.init_agent()
        
    def get_selectors(self, selector_filter="/"):
        return self.bus.get_selectors(self.id, selector_filter)
        
    def push(self, descriptor):
        self.bus.push(self.id, descriptor.selector, descriptor)
        self.log.info("pushed {0}".format(descriptor))
    def get(self, selector):
        return self.bus.get(self.id, selector)
    def lock(self, lockid, selector):
        return self.bus.lock(self.id, lockid, selector)

    def on_new_descriptor(self, sender_id, domain, selector):
        self.log.debug("Received from %s descriptor [%s:%s]" % (sender_id, domain,selector))
        if domain != self.domain:
            return
        if self.selector_filter(selector):
            if self.lock(self.name, selector):
                desc = self.get(selector)
                if self.name in desc.agents:
                    return # already processed
                if self.descriptor_filter(desc):
                    self.log.info("START Processing %r" % desc)
                    self.process(desc, sender_id)
                    self.log.info("END   processing %r" % desc)

    def mainloop(self):
        self.bus.mainloop(self.id)

    # These are the main methods that any agent would want
    # to overload
    def init_agent(self):
        pass
    def selector_filter(self, selector):
        return True
    def descriptor_filter(self, descriptor):
        return True
    def process(self, descriptor, sender_id):
        pass
    def run(self, options):
        pass
    @classmethod
    def add_arguments(cls, subparser):
        pass


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


