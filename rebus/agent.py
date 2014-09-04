from rebus.tools.registry import Registry
from rebus.bus import DEFAULT_DOMAIN
import logging
import time


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

    def __init__(self, bus, name=None, domain='default'):
        self.name = name if name else self._name_
        self.domain = domain
        self.bus = bus
        self.id = self.bus.join(self.name, domain,
                                callback=self.on_new_descriptor)
        self.log = AgentLogger(log, dict(agent_id=self.id))
        self.log.info('Agent {0.name} registered on bus {1._name_} '
                      'with id {0.id}'.format(self, self.bus))
        self.start_time = 0
        self.init_agent()

    def get_selectors(self, selector_filter="/"):
        return self.bus.get_selectors(self, selector_filter)

    def push(self, descriptor):
        if descriptor.processing_time == -1:
            descriptor.processing_time = time.time() - self.start_time
        result = self.bus.push(self, descriptor)
        self.log.debug("pushed {0}, already present: {1}".format(descriptor,
                                                                 not result))
        return result

    def get(self, desc_domain, selector):
        return self.bus.get(self, desc_domain, selector)

    def find(self, domain, selector_regex, limit=1):
        return self.bus.find(self, domain, selector_regex, limit)

    def lock(self, lockid, desc_domain, selector):
        return self.bus.lock(self, lockid, desc_domain, selector)

    def on_new_descriptor(self, sender_id, desc_domain, selector):
        self.log.debug("Received from %s descriptor [%s:%s]", sender_id,
                       desc_domain, selector)
        if self.domain != DEFAULT_DOMAIN and desc_domain != self.domain:
            return
        if self.selector_filter(selector):
            if self.lock(self.name, desc_domain, selector):
                desc = self.get(desc_domain, selector)
                # TODO detect infinite loops ?
                # if self.name in desc.agents:
                #     return  # already processed
                if self.descriptor_filter(desc):
                    self.log.info("START Processing %r" % desc)
                    self.process(desc, sender_id)
                    self.log.info("END   Processing %r" % desc)

    def run_in_bus(self, args):
        self.bus.run_agent(self, args)

    def agentloop(self):
        self.bus.agentloop(self)

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
        self.bus.agentloop(self)

    @classmethod
    def add_arguments(cls, subparser):
        pass
