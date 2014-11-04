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

    def join(self, name, agent_domain=DEFAULT_DOMAIN, callback=None):
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

    def processed_stats(self, agent_id, desc_domain):
        raise NotImplementedError

    def get_children(self, agent_id, desc_domain, selector, recurse=True):
        raise NotImplementedError

    def mainloop(self, agent_id):
        raise NotImplementedError
