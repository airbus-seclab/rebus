from rebus.tools.registry import Registry
import time

DEFAULT_DOMAIN = "default"


class BusRegistry(Registry):
    pass


class Bus(object):
    _name_ = "Bus"
    _desc_ = "N/A"

    @staticmethod
    def register(f):
        return BusRegistry.register_ref(f, key="_name_")

    # TODO: find a way to remove the heartbeat_interval
    def __init__(self, busaddr=None, heartbeat_interval=0):
        pass

    def join(self, agent, agent_domain=DEFAULT_DOMAIN):
        """
        Make an agent join the bus.

        :param agent: the agent which is joining the bus
        :param agent_domain: the domain this agent is interested in; use
            DEFAULT_DOMAIN for any domain.
        """
        raise NotImplementedError

    def lock(self, agent_id, lockid, desc_domain, selector):
        """
        Make sure no other agent having the same agent_id and lockid will also
        process this descriptor. This is especially useful when several
        instances of the same agent are running as a load-balancing mechanism.

        :param agent_id: current agent id
        :param lockid: lock string, typically built from the agent's name and
            its output-altering options
        :param desc_domain: domain the Descriptor being locked belongs to
        :param selector: selector of the Descriptor being locked
        """
        raise NotImplementedError

    def push(self, agent_id, descriptor):
        """
        Push a descriptor to the bus.

        :param descriptor: Descriptor object to be pushed to the bus
        :param agent_id: current agent id
        """
        raise NotImplementedError

    def get(self, agent_id, desc_domain, selector):
        """
        Get a Descriptor object from the bus.

        :param agent_id: current agent id
        :param desc_domain: domain the descriptor being fetched belongs to
        :param selector: selector of the descriptor being fetched
        """
        raise NotImplementedError

    def find_by_selector(self, agent_id, desc_domain, selector_prefix):
        """
        Return a list of all Descriptors whose selector match the provided
        prefix, belonging to the specified domain.

        :param agent_id: current agent id
        :param desc_domain: domain the Descriptors being searched belong to
        :param selector_prefix: search prefix for the Descriptors
        """
        raise NotImplementedError

    def get_value(self, agent_id, desc_domain, selector):
        """
        Return a descriptor's value.

        :param agent_id: current agent id
        :param desc_domain: domain the descriptor being fetched belongs to
        :param selector: selector of the descriptor being fetched
        """
        raise NotImplementedError

    def list_uuids(self, agent_id, desc_domain):
        """
        Return a dictionary mapping known UUIDs to corresponding labels.

        :param agent_id: current agent id
        :param desc_domain: domain from which UUID should be enumerated
        """
        raise NotImplementedError

    def find(self, agent_id, desc_domain, selector_regex, limit):
        """
        Return a list of selectors according to search constraints:

        * domain
        * selector regular expression
        * limit (max number of entries to return)

        :param agent_id: current agent id
        :param desc_domain: string, domain in which the search is performed
        :param selector_regex: string, regex
        :param limit: int, max number of selectors to return. Unlimited if 0.

        Only selectors of *limit* most recently added descriptors will be
        returned, from most recent to oldest.
        """
        raise NotImplementedError

    def find_by_uuid(self, agent_id, desc_domain, uuid):
        """
        Return a list of descriptors whose uuid match given parameter.
        Unspecified list order - may vary depending on the backend.

        :param agent_id: current agent id
        :param desc_domain: string, domain in which to look for descriptors
        :param uuid: uuid in which descriptors should be searched
        """
        raise NotImplementedError

    def find_by_value(self, agent_id, desc_domain, selector_prefix,
                      value_regex):
        """
        Return a list of matching descriptors:
        * desc.domain == desc_domain
        * desc.selector.startswith(selector_prefix)
        * re.match(value_regex, desc.value)

        :param agent_id: current agent id
        :param desc_domain: string, domain in which to look for descriptors
        :param selector_prefix: search prefix for the Descriptors
        :param value_regex: regex that the Descriptors' values should match
        """
        raise NotImplementedError

    def mark_processed(self, agent_id, desc_domain, selector):
        """
        Called every time an agent has processed a descriptor.

        :param agent_id: current agent id
        :param desc_domain: string, domain on which operations are performed
        :param selector: selector of the descriptor that has been processed
        """
        raise NotImplementedError

    def mark_processable(self, agent_id, desc_domain, selector):
        """
        Called by agents that are running in interactive mode, when selector
        passes their selector_filter.

        :param agent_id: current agent id
        :param desc_domain: string, domain on which operations are performed
        :param selector: selector of the descriptor that has been marked as
            processable
        """
        raise NotImplementedError

    def get_processable(self, agent_id, desc_domain, selector):
        """
        Return a list of (agent, config_txt) running in interactive mode that
        could process this selector.

        :param agent_id: current agent id
        :param desc_domain: string, domain this selector belongs to
        :param selector: string
        """
        raise NotImplementedError

    def list_agents(self, agent_id):
        """
        Return a dictionary mapping agent names to number of currently
        running instances.

        :param agent_id: current agent id
        """
        raise NotImplementedError

    def processed_stats(self, agent_id, desc_domain):
        """
        Return a list of couples, (agent names, number of processed selectors)
        and the total amount of selectors in this domain.

        :param agent_id: current agent id
        :param desc_domain: string, domain for which stats should be retrieved
        """
        raise NotImplementedError

    def get_children(self, agent_id, desc_domain, selector, recurse=True):
        """
        Return a set of children descriptors from given selector.

        :param agent_id: current agent id
        :param desc_domain: string, domain for which stats should be retrieved
        :param selector: string
        :param recurse: boolean, recursively fetch children if True
        """
        raise NotImplementedError

    def store_internal_state(self, agent_id, state):
        """
        Called by agents that need their serialized internal state to be
        stored.

        :param agent_id: current agent id
        :param state: Agent's serialized state. Will not be interpreted by the
            bus or storage
        """
        raise NotImplementedError

    def load_internal_state(self, agent_id):
        """
        Called by agents to fetch their serialized internal state in order to
        restore it.

        :param agent_id: current agent id
        """
        raise NotImplementedError

    def request_processing(self, agent_id, desc_domain, selector, targets):
        """
        Request that described descriptor (domain, selector) be processed by
        agents whose name is in targets.

        :param agent_id: id of the requesting agent
        :param desc_domain: descriptor domain
        :param selector: descriptor selector
        :param targets: list of target agent names.

        TODO if needed, add support for config_txt - target only agents whose
        configuration is provided?
        """
        raise NotImplementedError

    def busthread_call(self, method, **params):
        """
        Request that method be called in the bus thread's context.

        :param method: method to call
        :param **params: dictionary of parameters
        """
        raise NotImplementedError

    def run_agents(self):
        """
        Runs all agents that have been added to the bus previously.
        """
        raise NotImplementedError

    def agent_process(self, agent, *args, **kargs):
        """
        Call agent's call_process method.
        Used to implement the "parallelize" feature in some buses.
        """
        agent.call_process(*args, **kargs)

    def sleep(self, t):
        """
        Call by the agent when it need to wait
        Used to reimplement the standard time.sleep() function

        :param time: The time to sleep (seconds).
        """
        time.sleep(t)
