import os
import hashlib
import cPickle
import dbus
import dbus.glib
from dbus.mainloop.glib import DBusGMainLoop
import dbus.service
import gobject


class REdescriptor(object):
    def __init__(self, selector, value, domain = "default", ):
        self.agents = []
        self.precursors = []
        self.hash = hashlib.sha256(value).hexdigest()
        self.selector = os.path.join(selector, self.hash)
        self.value = value
        self.domain = domain

    def new_descriptor(self, selector, value, agent):
        desc = self.__class__(selector, value, self.domain)
        desc.agents += self.agents
        desc.agents.append(agent)
        desc.precursors += self.precursors
        desc.precursors.append(self.selector)
        return desc

    def __repr__(self):
        return "%s:%s=[len=%i]%r" % (self.domain, self.selector,
                                     len(self.value), self.value[:20])



class REagent(dbus.service.Object):
    def __init__(self, name, domain="default", bus=None):
        bus = bus if bus is not None else dbus.SessionBus()
        self.objpath = os.path.join("/agent", name)

        self.name = name
        self.well_known_name = dbus.service.BusName("com.airbus.rebus.agent.%s" % name, bus)
        dbus.service.Object.__init__(self, bus, self.objpath)
        self.bus = bus
        
        self.agent_id = self.bus.get_unique_name()
        self.domain = domain

        self.rebus = self.bus.get_object("com.airbus.rebus.bus", "/bus")
        self.iface = dbus.Interface(self.rebus, "com.airbus.rebus.bus")
        self.bus.add_signal_receiver(self.new_descriptor,
                                     dbus_interface="com.airbus.rebus.bus",
                                     signal_name="new_descriptor")
        
        self.iface.register(domain, self.objpath)
        
    def get_past_descriptors(self, selector="/"):
        return self.iface.get_past_descriptors(selector)
        
    def push(self, descriptor):
        self.iface.push(descriptor.selector, cPickle.dumps(descriptor))
    def get(self, selector):
        return cPickle.loads(str(self.iface.get(selector)))
    def lock(self, selector):
        return self.iface.lock(selector)
        
    def selector_filter(self, selector):
        return True
    def descriptor_filter(self, descriptor):
        return True

    def new_descriptor(self, sender, domain, selector):
        if sender == self.agent_id: # Pushed by this agent
            return 
        if domain != self.domain:
            return
        if self.selector_filter(selector):
            if self.lock(selector):
                desc = self.get(selector)
                if self.name in desc.agents:
                    return # already processed
                if self.descriptor_filter(desc):
                    self.process(desc)

    def process(self, descriptor):
        raise NotImplemented("REagent.process()")
    
    def mainloop(self):
        mainloop = gobject.MainLoop()
        mainloop.run()


gobject.threads_init()
dbus.glib.init_threads()
DBusGMainLoop(set_as_default=True)

