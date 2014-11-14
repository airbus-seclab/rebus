from rebus.tools.registry import Registry

DEFAULT_DOMAIN = "default"


class BusRegistry(Registry):
    pass


class Bus(object):
    _name_ = "Bus"
    _desc_ = "N/A"

    @staticmethod
    def register(f):
        return BusRegistry.register_ref(f, key="_name_")

    def __init__(self, busaddr=None):
        pass

    def join(self, agent, agent_domain=DEFAULT_DOMAIN, callback=None):
        raise NotImplementedError

    def lock(self, agent_id, lockid, desc_domain, selector):
        raise NotImplementedError

    def push(self, agent_id, descriptor):
        raise NotImplementedError

    def get(self, agent_id, desc_domain, selector):
        raise NotImplementedError

    def get_value(self, agent_id, desc_domain, selector):
        raise NotImplementedError

    def list_uuids(self, agent_id, desc_domain):
        raise NotImplementedError

    def find(self, agent_id, desc_domain, selector_regex, limit):
        raise NotImplementedError

    def find_by_uuid(self, agent_id, desc_domain, uuid):
        raise NotImplementedError

    def mark_processed(self, desc_domain, selector, agent_id, config_txt):
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

    def mainloop(self, agent_id):
        raise NotImplementedError
