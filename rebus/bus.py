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

    def join(self, name, domain=DEFAULT_DOMAIN, callback=None):
        raise NotImplemented

    def lock(self, agent_id, lockid, selector):
        pass
    def get(self, agent_id, selector):
        pass
    def push(self, agent_id, selector, descriptor):
        pass
    def get_selectors(self, agent_id, selector):
        pass
    def mainloop(self, agent_id):
        pass
