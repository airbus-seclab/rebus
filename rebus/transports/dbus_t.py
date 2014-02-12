import os
import dbus
import dbus.glib
from dbus.mainloop.glib import DBusGMainLoop
import dbus.service
import gobject
import rebus.transport

import logging
log = logging.getLogger("rebus.transport.dbus")


gobject.threads_init()
dbus.glib.init_threads()
DBusGMainLoop(set_as_default=True)


class DBus( rebus.transport.Transport):
    def __init__(self, busaddr=None):
        rebus.transport.Transport.__init__(self)
        self.bus = dbus.SessionBus() if busaddr is None else dbus.bus.BusConnection(busaddr)
        self.rebus = self.bus.get_object("com.airbus.rebus.bus", "/bus")

    def join(self, name, domain="default", callback=None):
        self.objpath = os.path.join("/agent", name)
        self.obj = dbus.service.Object(self.bus, self.objpath)
        self.well_known_name = dbus.service.BusName("com.airbus.rebus.agent.%s" % name, self.bus)
        self.agent_id = self.bus.get_unique_name()

        self.iface = dbus.Interface(self.rebus, "com.airbus.rebus.bus")
        self.iface.register(domain, self.objpath)

        log.info("Agent %s registered with id %s on domain %s" 
                 % (name, self.agent_id, domain))
        
        if callback:
            self.bus.add_signal_receiver(callback,
                                         dbus_interface="com.airbus.rebus.bus",
                                         signal_name="new_descriptor")
        return self
    def mainloop(self):
        gobject.MainLoop().run()

    def lock(self, selector):
        return self.iface.lock(selector)
    def get(self, selector):
        return self.iface.get(selector)
    def push(self, selector, descriptor):
        return self.iface.push(selector, descriptor)
    def get_past_descriptors(self, selector):
        return self.iface.get_past_descriptors(selector)



