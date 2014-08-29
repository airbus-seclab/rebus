#! /usr/bin/env python


from collections import defaultdict
import dbus.service
import dbus.glib
from dbus.mainloop.glib import DBusGMainLoop
import rebus.storage
from rebus.descriptor import Descriptor
import gobject

import logging


log = logging.getLogger("rebus.bus")


class DBusMaster(dbus.service.Object):
    def __init__(self, bus, objpath):
        dbus.service.Object.__init__(self, bus, objpath)
        self.store = rebus.storage.DescriptorStorage()
        self.clients = {}
        self.processed = defaultdict(set)

    @dbus.service.method(dbus_interface='com.airbus.rebus.bus',
                         in_signature='sso', out_signature='')
    def register(self, agent_id, agent_domain, pth):
        self.clients[agent_id] = pth
        log.info("New client %s (%s) in domain %s", pth, agent_domain,
                 agent_id)

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
                         in_signature='sssu', out_signature='as')
    def find(self, agent_id, domain, selector_regex, limit):
        log.debug("FIND: %s %s:%s (%d)", agent_id, domain, selector_regex,
                  limit)
        return self.store.find(domain, selector_regex, limit)

    @dbus.service.method(dbus_interface='com.airbus.rebus.bus',
                         in_signature='ssss', out_signature='b')
    def lock(self, agent_id, lockid, desc_domain, selector):
        objpath = self.clients[agent_id]
        processed = self.processed[desc_domain]
        key = (lockid, desc_domain, selector)
        log.debug("LOCK:%s %s(%s) => %r %s:%s ", lockid, objpath, agent_id,
                  key in processed, desc_domain, selector)
        if key in processed:
            return False
        processed.add(key)
        return True

    @dbus.service.method(dbus_interface='com.airbus.rebus.bus',
                         in_signature='sss', out_signature='as')
    def get_children(self, agent_id, desc_domain, selector):
        log.debug("GET_CHILDREN: %s %s:%s", agent_id, desc_domain, selector)
        return list(self.store.get_children(str(desc_domain), str(selector),
                                            recurse=True, serialized=True))

    @dbus.service.signal(dbus_interface='com.airbus.rebus.bus',
                         signature='sss')
    def new_descriptor(self, sender_id, desc_domain, selector):
        pass

    @classmethod
    def run(cls):
        gobject.threads_init()
        dbus.glib.init_threads()
        DBusGMainLoop(set_as_default=True)

        bus = dbus.SessionBus()
        name = dbus.service.BusName("com.airbus.rebus.bus", bus)
        svc = cls(bus, "/bus")

        mainloop = gobject.MainLoop()
        log.info("Entering main loop.")
        mainloop.run()
