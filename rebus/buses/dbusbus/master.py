#! /usr/bin/env python

from collections import defaultdict
import dbus.service
import dbus.glib
from dbus.mainloop.glib import DBusGMainLoop
import gobject

import logging


log=logging.getLogger("rebus.bus")
logging.basicConfig(level=1)


class DBusMaster(dbus.service.Object):
    def __init__(self, bus, objpath):
        dbus.service.Object.__init__(self, bus, objpath)
        self.clients = {}
        self.domains = {}
        self.processed = defaultdict(set)
        self.descriptors = defaultdict(dict)

    @dbus.service.method(dbus_interface='com.airbus.rebus.bus',
                         in_signature='sso', out_signature='')
    def register(self, agent_id, domain, pth):
        self.domains[agent_id] = domain
        self.clients[agent_id] = pth
        log.info("New client %s (%s) in domain %s" % (pth, agent_id, domain))

    @dbus.service.method(dbus_interface='com.airbus.rebus.bus',
                         in_signature='ss', out_signature='as')
    def get_past_descriptors(self, agent_id, selector):
        domain = self.domains[agent_id]
        return [ d for s,d in self.descriptors[domain].iteritems()
                 if s.startswith(selector) ]

    @dbus.service.method(dbus_interface='com.airbus.rebus.bus',
                         in_signature='sss', out_signature='')
    def push(self, agent_id, selector, descriptor):
        domain = self.domains[agent_id]
        if selector not in self.descriptors[domain]:
            log.info("PUSH: %s => %s:%s" % (agent_id, domain, selector))
            self.descriptors[domain][selector] = descriptor
            self.new_descriptor(agent_id, domain, selector)
        else:
            log.info("PUSH: %s already seen => %s:%s" % (agent_id, domain, selector))

    @dbus.service.method(dbus_interface='com.airbus.rebus.bus',
                         in_signature='ss', out_signature='s')
    def get(self, agent_id, selector):
        domain = self.domains[agent_id]
        log.info("GET: %s %s:%s" % (agent_id, domain, selector))
        return self.descriptors[domain][selector]

    @dbus.service.method(dbus_interface='com.airbus.rebus.bus',
                         in_signature='sss', out_signature='b')
    def lock(self, agent_id, lockid, selector):
        domain = self.domains[agent_id]
        objpath = self.clients[agent_id]
        processed  = self.processed[domain]
        key = (lockid,selector)

        log.info("LOCK:%s %s(%s) => %r %s:%s "%(lockid, objpath, agent_id, key in processed, domain, selector))
        if key in processed:
            return False
        processed.add(key)
        return True

        
    @dbus.service.signal(dbus_interface='com.airbus.rebus.bus',
                         signature='sss')
    def new_descriptor(self, sender_id, domain, selector):
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

