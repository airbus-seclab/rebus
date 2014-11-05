#! /usr/bin/env python


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
        #: processed[domain] is a set of (lockid, selector) whose processing
        #: has started (might even be finished). Allows several agents that
        #: perform the same stateless computation to run in parallel
        self.processed = defaultdict(set)

    @dbus.service.method(dbus_interface='com.airbus.rebus.bus',
                         in_signature='sso', out_signature='')
    def register(self, agent_id, agent_domain, pth):
        self.clients[agent_id] = pth
        log.info("New client %s (%s) in domain %s", pth, agent_domain,
                 agent_id)

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
                         in_signature='sssu', out_signature='as')
    def find(self, agent_id, desc_domain, selector_regex, limit):
        log.debug("FIND: %s %s:%s (%d)", agent_id, desc_domain, selector_regex,
                  limit)
        return self.store.find(desc_domain, selector_regex, limit)

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
        return self.store.list_uuids(desc_domain)

    @dbus.service.method(dbus_interface='com.airbus.rebus.bus',
                         in_signature='sss', out_signature='as')
    def find_by_uuid(self, agent_id, desc_domain, uuid):
        log.debug("FINDBYUUID: %s %s:%s", agent_id, desc_domain, uuid)
        return self.store.find_by_uuid(desc_domain, uuid, serialized=True)

    @dbus.service.method(dbus_interface='com.airbus.rebus.bus',
                         in_signature='ssss', out_signature='')
    def mark_processed(self, desc_domain, selector, agent_id, config_txt):
        log.debug("MARK_PROCESSED: %s:%s %s %s", desc_domain, selector,
                  agent_id, config_txt)
        self.store.mark_processed(desc_domain, selector, agent_id, config_txt)

    @dbus.service.method(dbus_interface='com.airbus.rebus.bus',
                         in_signature='ss', out_signature='a(su)u')
    def processed_stats(self, agent_id, desc_domain):
        log.debug("PROCESSED_STATS: %s %s", agent_id, desc_domain)
        return self.store.processed_stats(desc_domain)

    @dbus.service.method(dbus_interface='com.airbus.rebus.bus',
                         in_signature='sssb', out_signature='as')
    def get_children(self, agent_id, desc_domain, selector, recurse):
        log.debug("GET_CHILDREN: %s %s:%s", agent_id, desc_domain, selector)
        return list(self.store.get_children(str(desc_domain), str(selector),
                                            serialized=True, recurse=recurse))

    @dbus.service.signal(dbus_interface='com.airbus.rebus.bus',
                         signature='sss')
    def new_descriptor(self, sender_id, desc_domain, selector):
        pass

    @classmethod
    def run(cls, store):
        gobject.threads_init()
        dbus.glib.init_threads()
        DBusGMainLoop(set_as_default=True)

        bus = dbus.SessionBus()
        name = dbus.service.BusName("com.airbus.rebus.bus", bus)
        svc = cls(bus, "/bus", store)

        mainloop = gobject.MainLoop()
        log.info("Entering main loop.")
        mainloop.run()
