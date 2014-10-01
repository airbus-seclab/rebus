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
        self.bus = dbus.SessionBus() if busaddr is None else \
            dbus.bus.BusConnection(busaddr)
        self.rebus = self.bus.get_object("com.airbus.rebus.bus", "/bus")

    def join(self, name, agent_domain='default', callback=None):
        self.callback = callback
        self.objpath = os.path.join("/agent", name)
        self.obj = dbus.service.Object(self.bus, self.objpath)
        self.well_known_name = dbus.service.BusName("com.airbus.rebus.agent.%s"
                                                    % name, self.bus)
        self.agent_id = "%s-%s" % (name, self.bus.get_unique_name())

        self.iface = dbus.Interface(self.rebus, "com.airbus.rebus.bus")
        self.iface.register(self.agent_id, agent_domain, self.objpath)

        log.info("Agent %s registered with id %s on domain %s",
                 name, self.agent_id, agent_domain)

        if self.callback:
            self.bus.add_signal_receiver(self.callback_wrapper,
                                         dbus_interface="com.airbus.rebus.bus",
                                         signal_name="new_descriptor")
        return self.agent_id

    def lock(self, agent, lockid, desc_domain, selector):
        return self.iface.lock(agent.id, lockid, desc_domain, selector)

    def get(self, agent, desc_domain, selector):
        return Descriptor.unserialize(str(self.iface.get(agent.id, desc_domain,
                                                         selector)))

    def find(self, agent, domain, selector_regex, limit):
        return self.iface.find(agent.id, domain, selector_regex, limit)

    def find_by_uuid(self, agent, domain, uuid):
        return self.iface.find_by_uuid(agent.id, domain, uuid)

    def get_children(self, agent, desc_domain, selector, recurse=True):
        return [Descriptor.unserialize(str(s)) for s in
                self.iface.get_children(agent.id, desc_domain, selector, recurse)]

    def push(self, agent, descriptor):
        return self.iface.push(agent.id, descriptor.serialize())

    def get_selectors(self, agent, selector_filter):
        return self.iface.get_selectors(agent.id, selector_filter)

    def callback_wrapper(self, sender_id, desc_domain, selector):
        if sender_id != self.agent_id:
            self.callback(sender_id, desc_domain, selector)

    def run_agent(self, agent, args):
        agent.run(*args)

    def agentloop(self, agent):
        gobject.threads_init()
        dbus.glib.init_threads()
        DBusGMainLoop(set_as_default=True)
        log.info("Entering agent loop")
        loop = gobject.MainLoop()
        try:
            loop.run()
        except KeyboardInterrupt:
            loop.quit()

    def busloop(self):
        pass
