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
        raise NotImplemented
    def lock(self, agent_id, lockid, desc_domain, selector):
        raise NotImplemented
    def get(self, agent_id, desc_domain, selector):
        raise NotImplemented
    def get_children(self, agent_id, desc_domain, selector):
        raise NotImplemented
    def push(self, agent_id, descriptor):
        raise NotImplemented
    def get_selectors(self, agent_id, selector_filter):
        raise NotImplemented
    def mainloop(self, agent_id):
        raise NotImplemented
