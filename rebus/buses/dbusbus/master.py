#! /usr/bin/env python

import os
import signal
from collections import Counter, defaultdict
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
        #: maps agentid (ex. inject-:1.234) to object path (ex:
        #: /agent/inject)
        self.clients = {}
        self.exiting = False
        #: processed[domain] is a set of (lockid, selector) whose processing
        #: has started (might even be finished). Allows several agents that
        #: perform the same stateless computation to run in parallel
        self.processed = defaultdict(set)
        signal.signal(signal.SIGTERM, self.sigterm_handler)

    @dbus.service.method(dbus_interface='com.airbus.rebus.bus',
                         in_signature='ssos', out_signature='')
    def register(self, agent_id, agent_domain, pth, config_txt):
        #: indicates whether another instance of the same agent is already
        #: running
        agent_name = agent_id.split('-', 1)[0]
        already_running = any([k.startswith(agent_name+'-') for k in
                               self.clients.keys()])
        self.clients[agent_id] = pth
        log.info("New client %s (%s) in domain %s", pth, agent_id,
                 agent_domain)
        # Send not-yet processed descriptors to the agent...
        if not already_running:
            # ...unless another instance of the same agent has already been
            # started, and should be processing those descriptors
            unprocessed = self.store.list_unprocessed_by_agent(agent_name,
                                                               config_txt)
            for dom, sel in unprocessed:
                self.targeted_descriptor("storage", dom, sel, [agent_name])

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
                log.info("Expecting %u more agents to exit (ex. %s)",
                         len(self.clients), self.clients.keys()[0])

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
                         in_signature='ssss', out_signature='as')
    def find_by_value(self, agent_id, desc_domain, selector_prefix,
                      value_regex):
        log.debug("FINDBYVALUE: %s %s %s %s", agent_id, desc_domain,
                  selector_prefix, value_regex)
        return self.store.find_by_value(str(desc_domain), str(selector_prefix),
                                        str(value_regex), serialized=True)

    @dbus.service.method(dbus_interface='com.airbus.rebus.bus',
                         in_signature='ssss', out_signature='')
    def mark_processed(self, desc_domain, selector, agent_id, config_txt):
        log.debug("MARK_PROCESSED: %s:%s %s %s", desc_domain, selector,
                  agent_id, config_txt)
        self.store.mark_processed(str(desc_domain), str(selector),
                                  str(agent_id), str(config_txt))

    @dbus.service.method(dbus_interface='com.airbus.rebus.bus',
                         in_signature='', out_signature='a{su}')
    def list_agents(self, agent_id):
        log.debug("LIST_AGENTS: %s", agent_id)
        #: maps agent name to number of instances of this agent
        counts = dict(Counter(objpath.rsplit('/', 1)[1] for objpath in
                              self.clients.values()))
        return counts

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
                         signature='sssas')
    def targeted_descriptor(self, sender_id, desc_domain, selector, targets):
        """
        Signal sent when a descriptor is sent to some target agents (not
        broadcast).
        Useful for:

        * Forcefully replaying a descriptor (debug purposes, or user request)
        * Feeding descriptors to a new agent. Used when resuming the bus.
        * Interactive mode - user may choose which selectors get send to each
          agent

        :param sender_id: sender id
        :param desc_domain: descriptor domain
        :param selector: descriptor selector
        :param targets: list of target agent names. Agents not in this list
          should ignore this descriptor.
        """
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
                log.info("Expecting %u more agents to exit (ex. %s)",
                         len(svc.clients), svc.clients.keys()[0])
                svc.bus_exit(store.STORES_INTSTATE)
                svc.mainloop.run()
        log.info("Stopping storage...")
        store.exit()

    @staticmethod
    def sigterm_handler(sig, frame):
        log.info("Caught Sigterm, exiting.")
        os._exit(1)
