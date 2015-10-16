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

    #TODO: find a way to remove the heartbeat_interval
    def __init__(self, busaddr=None, heartbeat_interval=0):
        pass

    def join(self, agent, agent_domain=DEFAULT_DOMAIN):
        raise NotImplementedError

    def lock(self, agent_id, lockid, desc_domain, selector):
        raise NotImplementedError

    def push(self, agent_id, descriptor):
        raise NotImplementedError

    def get(self, agent_id, desc_domain, selector):
        raise NotImplementedError

    def get_value(self, agent_id, desc_domain, selector):
        """
        Returns a descriptor's value.
        """
        raise NotImplementedError

    def list_uuids(self, agent_id, desc_domain):
        raise NotImplementedError

    def find(self, agent_id, desc_domain, selector_regex, limit):
        raise NotImplementedError

    def find_by_uuid(self, agent_id, desc_domain, uuid):
        raise NotImplementedError

    def find_by_value(self, agent_id, desc_domain, selector_prefix,
                      value_regex):
        """
        Returns a list of matching descriptors:
        * desc.domain == desc_domain
        * desc.selector.startswith(selector_prefix)
        * re.match(value_regex, desc.value)
        """
        raise NotImplementedError

    def mark_processed(self, agent_id, desc_domain, selector):
        raise NotImplementedError

    def mark_processable(self, agent_id, desc_domain, selector):
        """
        Called by agents that are running in interactive mode, when selector
        passes their selector_filter
        """
        raise NotImplementedError

    def get_processable(self, agent_id, desc_domain, selector):
        """
        Returns a list of (agent, config_txt) running in interactive mode that
        could process this selector.

        :param domain: string, domain this selectors belongs to
        :param selector: string
        """
        raise NotImplementedError

    def list_agents(self, agent_id):
        """
        Returns a dictionary mapping agent names to number of currently
        running instances.
        """
        raise NotImplementedError

    def processed_stats(self, agent_id, desc_domain):
        raise NotImplementedError

    def get_children(self, agent_id, desc_domain, selector, recurse=True):
        raise NotImplementedError

    def store_internal_state(self, agent_id, state):
        """
        Called by agents that need their serialized internal state to be
        stored.
        """
        raise NotImplementedError

    def load_internal_state(self, agent_id):
        """
        Called by agents to fetch their serialized internal state in order to
        restore it.
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

    def sleep(self, time):
        """
        Call by the agent when it need to wait
        Used to reimplement the standard time.sleep() function
        :param time: The time to sleep.
        """
        time.sleep(time)
