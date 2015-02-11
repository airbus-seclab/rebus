import os
import sys
import signal
import dbus
import dbus.glib
from dbus.mainloop.glib import DBusGMainLoop
import dbus.service
import gobject
from rebus.agent import Agent
from rebus.bus import Bus, DEFAULT_DOMAIN
from rebus.descriptor import Descriptor
import logging
log = logging.getLogger("rebus.bus.dbus")


@Bus.register
class DBus(Bus):
    _name_ = "dbus"
    _desc_ = "Use DBus to exchange messages by connecting to REbus master"

    # Bus methods implementations - same order as in bus.py
    def __init__(self, busaddr=None):
        Bus.__init__(self)
        self.bus = dbus.SessionBus() if busaddr is None else \
            dbus.bus.BusConnection(busaddr)
        self.rebus = self.bus.get_object("com.airbus.rebus.bus", "/bus")
        signal.signal(signal.SIGTERM, self.sigterm_handler)
        #: Contains agent instance. This Bus implementation accepts only one
        #: agent. Agent must be run using separate DBus() (bus slave)
        #: instances.
        self.agent = None
        self.callback = None
        self.loop = None

    def join(self, agent, agent_domain=DEFAULT_DOMAIN, callback=None):
        self.callback = callback
        self.agent = agent
        self.agentname = agent.name
        self.objpath = os.path.join("/agent", self.agentname)
        self.obj = dbus.service.Object(self.bus, self.objpath)
        self.well_known_name = dbus.service.BusName("com.airbus.rebus.agent.%s"
                                                    % self.agentname, self.bus)
        self.agent_id = "%s-%s" % (self.agentname, self.bus.get_unique_name())

        if self.callback:
            # Always true when called from Agent - even if agent does not
            # overload process()
            self.bus.add_signal_receiver(self.broadcast_callback_wrapper,
                                         dbus_interface="com.airbus.rebus.bus",
                                         signal_name="new_descriptor")
            self.bus.add_signal_receiver(self.targeted_callback_wrapper,
                                         dbus_interface="com.airbus.rebus.bus",
                                         signal_name="targeted_descriptor")
        self.bus.add_signal_receiver(self.bus_exit_handler,
                                     dbus_interface="com.airbus.rebus.bus",
                                     signal_name="bus_exit")

        self.iface = dbus.Interface(self.rebus, "com.airbus.rebus.bus")
        self.iface.register(self.agent_id, agent_domain, self.objpath,
                            self.agent.config_txt)

        log.info("Agent %s registered with id %s on domain %s",
                 self.agentname, self.agent_id, agent_domain)

        return self.agent_id

    def lock(self, agent_id, lockid, desc_domain, selector):
        return bool(self.iface.lock(str(agent_id), lockid, desc_domain,
                                    selector))

    def push(self, agent_id, descriptor):
        return bool(self.iface.push(str(agent_id), descriptor.serialize()))

    def get(self, agent_id, desc_domain, selector):
        return Descriptor.unserialize(str(
            self.iface.get(str(agent_id), desc_domain, selector)), bus=self)

    def get_value(self, agent_id, desc_domain, selector):
        return Descriptor.unserialize_value(str(
            self.iface.get_value(str(agent_id), desc_domain, selector)))

    def list_uuids(self, agent_id, desc_domain):
        return {str(k): str(v) for k, v in
                self.iface.list_uuids(str(agent_id), desc_domain).items()}

    def find(self, agent_id, desc_domain, selector_regex, limit):
        return [str(i) for i in
                self.iface.find(str(agent_id), desc_domain, selector_regex,
                                limit)]

    def find_by_uuid(self, agent_id, desc_domain, uuid):
        return [Descriptor.unserialize(str(s), bus=self) for s in
                self.iface.find_by_uuid(str(agent_id), desc_domain, uuid)]

    def find_by_value(self, agent_id, desc_domain, selector_prefix,
                      value_regex):
        return [Descriptor.unserialize(str(s), bus=self) for s in
                self.iface.find_by_value(str(agent_id), desc_domain,
                                         selector_prefix, value_regex)]

    def mark_processed(self, desc_domain, selector, agent_id, config_txt):
        self.iface.mark_processed(desc_domain, selector, agent_id, config_txt)

    def mark_processable(self, desc_domain, selector, agent_id, config_txt):
        self.iface.mark_processable(desc_domain, selector, agent_id,
                                    config_txt)

    def list_agents(self, agent_id):
        return {str(k): int(v) for k, v in
                self.iface.list_agents(agent_id).items()}

    def processed_stats(self, agent_id, desc_domain):
        stats, total = self.iface.processed_stats(str(agent_id), desc_domain)
        return [(str(k), int(v)) for k, v in stats], int(total)

    def get_children(self, agent_id, desc_domain, selector, recurse=True):
        return [Descriptor.unserialize(str(s), bus=self) for s in
                self.iface.get_children(str(agent_id), desc_domain, selector,
                                        recurse)]

    def store_internal_state(self, agent_id, state):
        self.iface.store_internal_state(agent_id, state)

    def load_internal_state(self, agent_id):
        return str(self.iface.load_internal_state(agent_id))

    def run_agents(self):
        self.agent.run()
        if self.agent.__class__.run != Agent.run:
            # the run() method has been overridden - agent will run on his own
            # then quit
            self.iface.unregister(self.agent_id)
            return
        gobject.threads_init()
        dbus.glib.init_threads()
        DBusGMainLoop(set_as_default=True)
        log.info("Entering agent loop")
        self.loop = gobject.MainLoop()
        try:
            self.loop.run()
        except (KeyboardInterrupt, SystemExit):
            self.loop.quit()
        self.iface.unregister(self.agent_id)

    # DBus specific functions
    def broadcast_callback_wrapper(self, sender_id, desc_domain, selector):
        self.callback(str(sender_id), str(desc_domain), str(selector))

    def targeted_callback_wrapper(self, sender_id, desc_domain, selector,
                                  targets):
        if self.agentname in targets:
            self.callback(str(sender_id), str(desc_domain), str(selector))

    def bus_exit_handler(self, awaiting_internal_state):
        if awaiting_internal_state:
            self.agent.save_internal_state()
        if self.loop:
            self.loop.quit()

    @staticmethod
    def sigterm_handler(sig, frame):
        log.info("Caught Sigterm, unregistering and exiting.")
        sys.exit(0)
