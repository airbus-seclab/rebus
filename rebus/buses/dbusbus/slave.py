import os
import dbus
import dbus.glib
from dbus.mainloop.glib import DBusGMainLoop
import dbus.service
import gobject
from rebus.bus import Bus
from rebus.descriptor import Descriptor

import logging
log = logging.getLogger("rebus.bus.dbus")




@Bus.register
class DBus(Bus):
    _name_ = "dbus"
    _desc_ = "Use DBus to exchange messages by connecting to REbus master"
    def __init__(self, busaddr=None):
        Bus.__init__(self)
        self.bus = dbus.SessionBus() if busaddr is None else dbus.bus.BusConnection(busaddr)
        self.rebus = self.bus.get_object("com.airbus.rebus.bus", "/bus")

    def join(self, name, domain="default", callback=None):
        self.callback = callback
        self.objpath = os.path.join("/agent", name)
        self.obj = dbus.service.Object(self.bus, self.objpath)
        self.well_known_name = dbus.service.BusName("com.airbus.rebus.agent.%s" % name, self.bus)
        self.agent_id = "%s-%s" % (name, self.bus.get_unique_name())

        self.iface = dbus.Interface(self.rebus, "com.airbus.rebus.bus")
        self.iface.register(self.agent_id, domain, self.objpath)

        log.info("Agent %s registered with id %s on domain %s"
                 % (name, self.agent_id, domain))

        if self.callback:
            self.bus.add_signal_receiver(self.callback_wrapper,
                                         dbus_interface="com.airbus.rebus.bus",
                                         signal_name="new_descriptor")
        return self.agent_id

    def lock(self, agent, lockid, selector):
        return self.iface.lock(agent.id, lockid, selector)
    def get(self, agent, selector):
        return Descriptor.unserialize(str(self.iface.get(agent.id, selector)))
    def push(self, agent, selector, descriptor):
        return self.iface.push(agent.id, selector, descriptor.serialize())
    def get_selectors(self, agent, selector_filter):
        return self.iface.get_selectors(agent.id, selector)
    def get_past_descriptors(self, agent, selector_filter):
        dlist = self.iface.get_past_descriptors(agent.id, selector_filter)
        return [Descriptor.unserialize(str(d)) for d in dlist]

    def callback_wrapper(self, sender_id, domain, selector):
        if sender_id != self.agent_id:
            self.callback(sender_id, domain, selector)

    def run_agent(self, agent, args):
        agent.run(*args)
    def agentloop(self, agent):
        gobject.threads_init()
        dbus.glib.init_threads()
        DBusGMainLoop(set_as_default=True)
        log.info("Entering agent loop")
        gobject.MainLoop().run()
    def busloop(self):
        pass

