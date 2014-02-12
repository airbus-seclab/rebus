class Transport(object):
    def __init__(self):
        pass
    def join(self, name, domain="default"):
        pass
    def register_callback(self, handle):
        pass

    def lock(self, selector):
        pass
    def get(self, selector):
        pass
    def push(self, selector, descriptor):
        pass
    def get_past_descriptors(self, selector):
        pass
    def mainloop(self):
        pass
