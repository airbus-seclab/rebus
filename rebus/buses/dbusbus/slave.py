import os
import sys
import signal
import dbus
import dbus.mainloop.glib
import dbus.service
import logging
import gobject
import thread
import time
from rebus.agent import Agent
from rebus.bus import Bus, DEFAULT_DOMAIN
from rebus.descriptor import Descriptor
from rebus.tools.serializer import b64serializer as serializer
log = logging.getLogger("rebus.bus.dbus")
DEFAULT_BUS = "(local dbus instance)"


@Bus.register
class DBus(Bus):
    _name_ = "dbus"
    _desc_ = "Use DBus to exchange messages by connecting to REbus master"

    # Bus methods implementations - same order as in bus.py
    def __init__(self, options):
        gobject.threads_init()
        dbus.mainloop.glib.threads_init()
        dbus.mainloop.glib.DBusGMainLoop(set_as_default=True)
        Bus.__init__(self)
        busaddr = options.busaddr
        self.bus = dbus.SessionBus() if busaddr == DEFAULT_BUS else \
            dbus.bus.BusConnection(busaddr)
        counter = 20
        while not (counter == 0):
            try:
                self.rebus = self.bus.get_object("com.airbus.rebus.bus",
                                                 "/bus")
                counter = 0
            except dbus.exceptions.DBusException as e:
                log.warning("Cannot get bus object's because : " + str(e) +
                            " : wait 5s and retry")
                counter = counter - 1
                time.sleep(5)

        signal.signal(signal.SIGTERM, self.sigterm_handler)
        #: Contains agent instance. This Bus implementation accepts only one
        #: agent. Agent must be run using separate DBus() (bus slave)
        #: instances.
        self.agent = None
        self.loop = None
        self.main_thread_id = thread.get_ident()

    def join(self, agent, agent_domain=DEFAULT_DOMAIN):
        self.agent = agent
        self.objpath = os.path.join("/agent", self.agent.name)
        self.obj = dbus.service.Object(self.bus, self.objpath)
        self.well_known_name = dbus.service.BusName("com.airbus.rebus.agent.%s"
                                                    % self.agent.name,
                                                    self.bus)
        self.agent_id = "%s-%s" % (self.agent.name, self.bus.get_unique_name())

        self.bus.add_signal_receiver(self.broadcast_wrapper,
                                     dbus_interface="com.airbus.rebus.bus",
                                     signal_name="new_descriptor")
        self.bus.add_signal_receiver(self.targeted_wrapper,
                                     dbus_interface="com.airbus.rebus.bus",
                                     signal_name="targeted_descriptor")
        self.bus.add_signal_receiver(self.bus_exit_handler,
                                     dbus_interface="com.airbus.rebus.bus",
                                     signal_name="bus_exit")
        # Pass "on_idle" signal to the agent as a method call
        self.bus.add_signal_receiver(self.agent.on_idle,
                                     dbus_interface="com.airbus.rebus.bus",
                                     signal_name="on_idle")

        self.iface = dbus.Interface(self.rebus, "com.airbus.rebus.bus")
        registerSucceed = False
        while not registerSucceed:
            try:
                self.iface.register(self.agent_id, agent_domain, self.objpath,
                                    self.agent.config_txt)
                registerSucceed = True
            except dbus.exceptions.DBusException as e:
                log.warning("Cannot register because of " + str(e) +
                            " : wait 1s and retry")
                time.sleep(1)

        log.info("Agent %s registered with id %s on domain %s",
                 self.agent.name, self.agent_id, agent_domain)

        return self.agent_id

    def lock(self, agent_id, lockid, desc_domain, selector):
        return bool(self.iface.lock(str(agent_id), lockid, desc_domain,
                                    selector))

    def push(self, agent_id, descriptor):
        if thread.get_ident() == self.main_thread_id:
            self._push(str(agent_id), descriptor)
        else:
            self.busthread_call(self._push, str(agent_id), descriptor)

    def _push(self, agent_id, descriptor):
        sd = descriptor.serialize(serializer)
        # Arbitrary size based on the true limit of 134217728 bytes
        # The true limit apply to the total size (message + header)
        #  -> I don't know the size of the message header
        if len(sd) > 134210000:
            log.warning("Descriptor too long for Dbus : " + str(len(sd)) +
                        " bytes")
            return False
        return bool(self.iface.push(str(agent_id), sd))

    def get(self, agent_id, desc_domain, selector):
        result = str(self.iface.get(str(agent_id), desc_domain, selector))
        if result == "":
            return None
        return Descriptor.unserialize(serializer, str(result), bus=self)

    def get_value(self, agent_id, desc_domain, selector):
        result = str(self.iface.get_value(str(agent_id), desc_domain,
                                          selector))
        if result == "":
            return None
        return Descriptor.unserialize_value(serializer, result)

    def list_uuids(self, agent_id, desc_domain):
        return {str(k): str(v) for k, v in
                self.iface.list_uuids(str(agent_id), desc_domain).items()}

    def find(self, agent_id, desc_domain, selector_regex, limit):
        return [str(i) for i in
                self.iface.find(str(agent_id), desc_domain, selector_regex,
                                limit)]

    def find_by_uuid(self, agent_id, desc_domain, uuid):
        return [Descriptor.unserialize(serializer, str(s), bus=self) for s in
                self.iface.find_by_uuid(str(agent_id), desc_domain, uuid)]

    def find_by_value(self, agent_id, desc_domain, selector_prefix,
                      value_regex):
        return [Descriptor.unserialize(serializer, str(s), bus=self) for s in
                self.iface.find_by_value(str(agent_id), desc_domain,
                                         selector_prefix, value_regex)]

    def find_by_selector(self, agent_id, desc_domain, selector_prefix):
        return [Descriptor.unserialize(serializer, str(s), bus=self) for s in
                self.iface.find_by_selector(str(agent_id), desc_domain,
                                            selector_prefix)]

    def mark_processed(self, agent_id, desc_domain, selector):
        self.iface.mark_processed(str(agent_id), desc_domain, selector)

    def mark_processable(self, agent_id, desc_domain, selector):
        self.iface.mark_processable(str(agent_id), desc_domain, selector)

    def get_processable(self, agent_id, desc_domain, selector):
        return [(str(agent_name), str(config_txt)) for (agent_name, config_txt)
                in self.iface.get_processable(str(agent_id), desc_domain,
                                              selector)]

    def list_agents(self, agent_id):
        return {str(k): int(v) for k, v in
                self.iface.list_agents(str(agent_id)).items()}

    def processed_stats(self, agent_id, desc_domain):
        stats, total = self.iface.processed_stats(str(agent_id), desc_domain)
        return [(str(k), int(v)) for k, v in stats], int(total)

    def get_children(self, agent_id, desc_domain, selector, recurse=True):
        return [Descriptor.unserialize(serializer, str(s), bus=self) for s in
                self.iface.get_children(str(agent_id), desc_domain, selector,
                                        recurse)]

    def store_internal_state(self, agent_id, state):
        self.iface.store_internal_state(str(agent_id), state)

    def load_internal_state(self, agent_id):
        return str(self.iface.load_internal_state(str(agent_id)))

    def request_processing(self, agent_id, desc_domain, selector, targets):
        self.iface.request_processing(str(agent_id), desc_domain, selector,
                                      targets)

    def busthread_call(self, method, *args):
        gobject.idle_add(method, *args)

    def run_agents(self):
        self.agent.run_and_catch_exc()
        if self.agent.__class__.run != Agent.run:
            # the run() method has been overridden - agent will run on his own
            # then quit
            self.iface.unregister(self.agent_id)
            return
        log.info("Entering agent loop")
        self.loop = gobject.MainLoop()
        try:
            self.loop.run()
        except (KeyboardInterrupt, SystemExit):
            self.loop.quit()
        # Clean up signals - useful for tests, where one process runs several
        # agents successively
        self.bus.remove_signal_receiver(self.broadcast_wrapper,
                                        dbus_interface="com.airbus.rebus.bus",
                                        signal_name="new_descriptor")
        self.bus.remove_signal_receiver(self.targeted_wrapper,
                                        dbus_interface="com.airbus.rebus.bus",
                                        signal_name="targeted_descriptor")
        self.bus.remove_signal_receiver(self.bus_exit_handler,
                                        dbus_interface="com.airbus.rebus.bus",
                                        signal_name="bus_exit")
        self.iface.unregister(self.agent_id)
        self.agent.save_internal_state()

    # DBus specific functions
    def broadcast_wrapper(self, sender_id, desc_domain, uuid, selector):
        self.agent.on_new_descriptor(str(sender_id), str(desc_domain),
                                     str(uuid), str(selector), 0)

    def targeted_wrapper(self, sender_id, desc_domain, uuid, selector, targets,
                         user_request):
        if self.agent.name in targets:
            self.agent.on_new_descriptor(str(sender_id), str(desc_domain),
                                         str(uuid), str(selector),
                                         int(user_request))

    def bus_exit_handler(self, awaiting_internal_state):
        if awaiting_internal_state:
            self.agent.save_internal_state()
        if self.loop:
            self.loop.quit()

    @staticmethod
    def sigterm_handler(sig, frame):
        log.info("Caught Sigterm, unregistering and exiting.")
        sys.exit(0)

    def agent_process(self, agent, *args, **kargs):
        if hasattr(self.agent, "_parallelize_"):
            log.debug("======> run in thread!!! params=%r",
                      self.agent._parallelize_)
            thread.start_new_thread(self.agent.call_process, args, kargs)
        else:
            self.agent.call_process(*args, **kargs)

    @staticmethod
    def add_arguments(subparser):
        subparser.add_argument(
            "--busaddr", help="URL of the dbus server",
            default=DEFAULT_BUS)
