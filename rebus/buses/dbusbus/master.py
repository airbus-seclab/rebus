#! /usr/bin/env python


import sys
import signal
from collections import defaultdict
import dbus.service
import dbus.glib
from dbus.mainloop.glib import DBusGMainLoop
from rebus.descriptor import Descriptor
import gobject

import logging


log = logging.getLogger("rebus.bus")


class DBusMaster(dbus.service.Object):
    def __init__(self, bus, objpath, store):
        dbus.service.Object.__init__(self, bus, objpath)
        self.store = store
        self.clients = {}
        self.exiting = False
        #: processed[domain] is a set of (lockid, selector) whose processing
        #: has started (might even be finished). Allows several agents that
        #: perform the same stateless computation to run in parallel
        self.processed = defaultdict(set)
        signal.signal(signal.SIGTERM, self.sigterm_handler)

    @dbus.service.method(dbus_interface='com.airbus.rebus.bus',
                         in_signature='sso', out_signature='')
    def register(self, agent_id, agent_domain, pth):
        self.clients[agent_id] = pth
        log.info("New client %s (%s) in domain %s", pth, agent_id,
                 agent_domain)

    @dbus.service.method(dbus_interface='com.airbus.rebus.bus',
                         in_signature='s', out_signature='')
    def unregister(self, agent_id):
        log.info("Agent %s has unregistered", agent_id)
        del self.clients[agent_id]
        if self.exiting:
            if len(self.clients) == 0:
                log.info("Exiting - no agents are running")
                self.mainloop.quit()
            else:
                log.info("Expecting %u more agents to exit (ex. %s)" %
                         (len(self.clients), self.clients.keys()[0]))

    @dbus.service.method(dbus_interface='com.airbus.rebus.bus',
                         in_signature='ssss', out_signature='b')
    def lock(self, agent_id, lockid, desc_domain, selector):
        objpath = self.clients[agent_id]
        processed = self.processed[desc_domain]
        key = (lockid, selector)
        log.debug("LOCK:%s %s(%s) => %r %s:%s ", lockid, objpath, agent_id,
                  key in processed, desc_domain, selector)
        if key in processed:
            return False
        processed.add(key)
        return True

    @dbus.service.method(dbus_interface='com.airbus.rebus.bus',
                         in_signature='ss', out_signature='b')
    def push(self, agent_id, descriptor):
        unserialized_descriptor = Descriptor.unserialize(str(descriptor))
        desc_domain = str(unserialized_descriptor.domain)
        selector = str(unserialized_descriptor.selector)
        if self.store.add(unserialized_descriptor,
                          serialized_descriptor=str(descriptor)):
            log.debug("PUSH: %s => %s:%s", agent_id, desc_domain, selector)
            self.new_descriptor(agent_id, desc_domain, selector)
            return True
        else:
            log.debug("PUSH: %s already seen => %s:%s", agent_id, desc_domain,
                      selector)
            return False

    @dbus.service.method(dbus_interface='com.airbus.rebus.bus',
                         in_signature='sss', out_signature='s')
    def get(self, agent_id, desc_domain, selector):
        log.debug("GET: %s %s:%s", agent_id, desc_domain, selector)
        return self.store.get_descriptor(str(desc_domain), str(selector),
                                         serialized=True)

    @dbus.service.method(dbus_interface='com.airbus.rebus.bus',
                         in_signature='sss', out_signature='s')
    def get_value(self, agent_id, desc_domain, selector):
        log.debug("GETVALUE: %s %s:%s", agent_id, desc_domain, selector)
        return self.store.get_value(str(desc_domain), str(selector))

    @dbus.service.method(dbus_interface='com.airbus.rebus.bus',
                         in_signature='ss', out_signature='a{ss}')
    def list_uuids(self, agent_id, desc_domain):
        log.debug("LISTUUIDS: %s %s", agent_id, desc_domain)
        return self.store.list_uuids(str(desc_domain))

    @dbus.service.method(dbus_interface='com.airbus.rebus.bus',
                         in_signature='sssu', out_signature='as')
    def find(self, agent_id, desc_domain, selector_regex, limit):
        log.debug("FIND: %s %s:%s (%d)", agent_id, desc_domain, selector_regex,
                  limit)
        return self.store.find(str(desc_domain), str(selector_regex),
                               str(limit))

    @dbus.service.method(dbus_interface='com.airbus.rebus.bus',
                         in_signature='sss', out_signature='as')
    def find_by_uuid(self, agent_id, desc_domain, uuid):
        log.debug("FINDBYUUID: %s %s:%s", agent_id, desc_domain, uuid)
        return self.store.find_by_uuid(str(desc_domain), str(uuid),
                                       serialized=True)

    @dbus.service.method(dbus_interface='com.airbus.rebus.bus',
                         in_signature='ssss', out_signature='')
    def mark_processed(self, desc_domain, selector, agent_id, config_txt):
        log.debug("MARK_PROCESSED: %s:%s %s %s", desc_domain, selector,
                  agent_id, config_txt)
        self.store.mark_processed(str(desc_domain), str(selector),
                                  str(agent_id), str(config_txt))

    @dbus.service.method(dbus_interface='com.airbus.rebus.bus',
                         in_signature='ss', out_signature='a(su)u')
    def processed_stats(self, agent_id, desc_domain):
        log.debug("PROCESSED_STATS: %s %s", agent_id, desc_domain)
        return self.store.processed_stats(str(desc_domain))

    @dbus.service.method(dbus_interface='com.airbus.rebus.bus',
                         in_signature='sssb', out_signature='as')
    def get_children(self, agent_id, desc_domain, selector, recurse):
        log.debug("GET_CHILDREN: %s %s:%s", agent_id, desc_domain, selector)
        return list(self.store.get_children(str(desc_domain), str(selector),
                                            serialized=True,
                                            recurse=bool(recurse)))

    @dbus.service.method(dbus_interface='com.airbus.rebus.bus',
                         in_signature='ss', out_signature='')
    def store_internal_state(self, agent_id, state):
        log.debug("STORE_INTSTATE: %s", agent_id)
        if self.store.STORES_INTSTATE:
            self.store.store_state(str(agent_id), str(state))

    @dbus.service.method(dbus_interface='com.airbus.rebus.bus',
                         in_signature='s', out_signature='s')
    def load_internal_state(self, agent_id):
        log.debug("LOAD_INTSTATE: %s", agent_id)
        if self.store.STORES_INTSTATE:
            return self.store.load_state(str(agent_id))
        return ""

    @dbus.service.signal(dbus_interface='com.airbus.rebus.bus',
                         signature='sss')
    def new_descriptor(self, sender_id, desc_domain, selector):
        pass

    @dbus.service.signal(dbus_interface='com.airbus.rebus.bus',
                         signature='b')
    def bus_exit(self, awaiting_internal_state):
        """
        Signal sent when the bus is exiting.
        :param awaiting_internal_state: indicates whether agents must send
        their internal serialized state for storage.
        """
        self.exiting = True
        return

    @classmethod
    def run(cls, store):
        gobject.threads_init()
        dbus.glib.init_threads()
        DBusGMainLoop(set_as_default=True)

        bus = dbus.SessionBus()
        name = dbus.service.BusName("com.airbus.rebus.bus", bus)
        svc = cls(bus, "/bus", store)

        svc.mainloop = gobject.MainLoop()
        log.info("Entering main loop.")
        try:
            svc.mainloop.run()
        except (KeyboardInterrupt, SystemExit):
            if len(svc.clients) > 0:
                log.info("Stopping all agents")
                # Ask slave agents to shutdown nicely & save internal state
                log.info("Expecting %u more agents to exit (ex. %s)" %
                         (len(svc.clients), svc.clients.keys()[0]))
                svc.bus_exit(store.STORES_INTSTATE)
                svc.mainloop.run()
        log.info("Stopping storage...")
        store.exit()

    @staticmethod
    def sigterm_handler(signal, frame):
        log.info("Caught Sigterm, unregistering and exiting.")
        sys.exit(0)
