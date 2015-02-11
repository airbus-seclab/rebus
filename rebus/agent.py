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
    #: Supported operation modes. Actual operation mode is chosen at launch;
    #: may be changed on master bus' order.
    #: default mode as 1st element.
    _operationmodes_ = ('automatic', 'interactive')
    #: tuple of names of options that influence output values. If not
    #: overridden, every option except 'operationmode' will be considered as
    #: influencing the output.
    _output_altering_options_ = None

    @staticmethod
    def register(f):
        return AgentRegistry.register_ref(f, key="_name_")

    def __init__(self, bus, options, name=None, domain='default'):
        self.name = name if name else self._name_
        self.domain = domain
        self.bus = bus
        #: {key: value} containing relevant parameters that may influence the
        #: agent's outputs
        self.config = vars(options)
        # Chosen operation mode
        if len(self._operationmodes_) == 1:
            self.config['operationmode'] = self._operationmodes_[0]
        if self._output_altering_options_ is None:
            self.config['output_altering_options'] = self.config.keys()
            self.config['output_altering_options'].remove('operationmode')
        else:
            self.config['output_altering_options'] = \
                self._output_altering_options_
        self.id = self.bus.join(self, domain, callback=self.on_new_descriptor)
        self.log = AgentLogger(log, dict(agent_id=self.id))
        self.log.info('Agent {0.name} registered on bus {1._name_} '
                      'with id {0.id}'.format(self, self.bus))
        self.start_time = 0
        self.init_agent()
        self.restore_internal_state()

    def push(self, descriptor):
        if descriptor.processing_time == -1:
            descriptor.processing_time = time.time() - self.start_time
        result = self.bus.push(self.id, descriptor)
        self.log.debug("pushed {0}, already present: {1}".format(descriptor,
                                                                 not result))
        return result

    def get(self, desc_domain, selector):
        return self.bus.get(self.id, desc_domain, selector)

    def find(self, domain, selector_regex, limit):
        return self.bus.find(self.id, domain, selector_regex, limit)

    def list_uuids(self, desc_domain):
        return self.bus.list_uuids(self.id, desc_domain)

    def processed_stats(self, desc_domain):
        return self.bus.processed_stats(self.id, desc_domain)

    def lock(self, lockid, desc_domain, selector):
        return self.bus.lock(self.id, lockid, desc_domain, selector)

    def on_new_descriptor(self, sender_id, desc_domain, selector):
        self.log.debug("Received from %s descriptor [%s:%s]", sender_id,
                       desc_domain, selector)
        if self.domain != DEFAULT_DOMAIN and desc_domain != self.domain:
            # this agent only processes descriptors whose domain is self.domain
            self.bus.mark_processed(desc_domain, selector, self.id,
                                    self.config_txt)
            return
        if not self.selector_filter(selector):
            # not interested in this
            self.bus.mark_processed(desc_domain, selector, self.id,
                                    self.config_txt)
            return
        if not self.lock(self.name, desc_domain, selector):
            # processing has already been started by another instance of
            # the same agent
            return
        if self.config['operationmode'] == 'interactive':
            self.bus.mark_processable(desc_domain, selector, self.id,
                                      self.config_txt)
            return
        desc = self.get(desc_domain, selector)
        # TODO detect infinite loops ?
        # if self.name in desc.agents:
        #     return  # already processed
        if self.descriptor_filter(desc):
            self.log.info("START Processing %r", desc)
            self.start_time = time.time()
            self.process(desc, sender_id)
            done = time.time()
            self.log.info("END   Processing |%f| %r",
                          done-self.start_time, desc)
        self.bus.mark_processed(desc_domain, selector, self.id,
                                self.config_txt)

    @property
    def config_txt(self):
        return json.dumps(self.config, sort_keys=True)

    def declare_link(self, desc1, desc2, linktype, reason):
        """
        Helper function.
        Requests two new /link/ descriptors, then pushes them.
        :param desc1: Descriptor instance
        :param desc2: Descriptor instance
        :param linktype: word describing the type of the link, that will be
          part of the selector
        :param reason: Text description of the link reason
        """
        link1, link2 = desc1.create_links(desc2, self.name, linktype, reason)
        self.push(link1)
        self.push(link2)

    def get_value(self, descriptor):
        if hasattr(descriptor, 'value'):
            return descriptor.value
        else:
            # TODO request from storage if locally available - implement when
            # agent has a reference to possibly existent local storage
            return self.bus.get_value(self.id, descriptor.domain,
                                      descriptor.selector)
            # possible trade-off: store now-fetched value in descriptor

    def save_internal_state(self):
        """
        Send internal state to storage. Called at agent shutdown, if persistent
        storage is in use.
        """
        state = self.get_internal_state()
        if state:
            self.bus.store_internal_state(self.id, state)

    def restore_internal_state(self):
        """
        Retrieve internal state from storage.
        """
        state = self.bus.load_internal_state(self.id)
        if state:
            self.set_internal_state(state)

    # These are the main methods that any agent might want to overload
    def init_agent(self):
        """
        Called to initialize the agent, after it has joined the bus.

        The internal state will be restored (set_internal_state) afterwards if
        relevant.
        """
        pass

    def selector_filter(self, selector):
        return True

    def descriptor_filter(self, descriptor):
        return True

    def process(self, descriptor, sender_id):
        pass

    def run(self):
        """
        Overriden by agents that do not consume descriptors.
        """
        pass

    def get_internal_state(self):
        """
        Should be overridden by agents that have an internal state, which
        should be persistent stored when an agent is stopped.

        Return a string that contains the internal agent state (ex. serialized
        data structures)
        """
        return

    def set_internal_state(self, state):
        """
        Should be overridden by agents that have an internal state, which
        should be persistently stored when an agent is stopped.

        :param state: string that contains the internal agent state (ex.
          serialized data structures)
        """
        return

    def __repr__(self):
        return self.id

    @classmethod
    def add_agent_arguments(cls, subparser):
        if len(cls._operationmodes_) > 1:
            subparser.add_argument('--mode', default=cls._operationmodes_[0],
                                   dest='operationmode',
                                   choices=cls._operationmodes_)
        cls.add_arguments(subparser)

    @classmethod
    def add_arguments(cls, subparser):
        """
        Overridden by agents that have configuration parameters
        """
        pass
