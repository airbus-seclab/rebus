from rebus.descriptor import Descriptor
from rebus.tools.registry import Registry
from rebus.bus import DEFAULT_DOMAIN
import logging
import time
import json


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
        # {key: value} containing relevant parameters that may influence the
        # agent's outputs
        self.config = dict()
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

    def find(self, domain, selector_regex, limit):
        return self.bus.find(self, domain, selector_regex, limit)

    def list_uuids(self, desc_domain):
        return self.bus.list_uuids(self, desc_domain)

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
                    self.start_time = time.time()
                    self.process(desc, sender_id)
                    done = time.time()
                    self.log.info("END   Processing |%f| %r" %
                                  (done-self.start_time, desc))
        config_txt = json.dumps(self.config, sort_keys=True)
        self.bus.mark_processed(desc_domain, selector, self.name, config_txt)

    def run_in_bus(self, args):
        self.bus.run_agent(self, args)

    def agentloop(self):
        self.bus.agentloop(self)

    # These are the main methods that any agent might want to overload
    def init_agent(self):
        pass

    def declare_link(self, desc1, desc2, linktype, reason):
        """
        Helper function.
        Requests two new /link/ descriptors, then pushes them.
        :param desc1: Descriptor instance
        :param desc2: Descriptor instance
        :param lintype: word describing the type of the link, that will be part of the selector
        :param reason: Text description of the link reason
        """
        link1, link2 = desc1.create_links(desc2, self.name, linktype, reason)
        self.push(link1)
        self.push(link2)

    def selector_filter(self, selector):
        return True

    def descriptor_filter(self, descriptor):
        return True

    def process(self, descriptor, sender_id):
        pass

    def run(self, options):
        self.bus.agentloop(self)

    def get_value(self, descriptor):
        if hasattr(descriptor, 'value'):
            return descriptor.value
        else:
            # TODO request from storage if locally available - implement when
            # agent has a reference to maybe existent local storage
            return self.bus.get_value(self, descriptor.domain,
                    descriptor.selector)
            # possible trade-off: store now-fetched value in descriptor


    @classmethod
    def add_arguments(cls, subparser):
        pass
