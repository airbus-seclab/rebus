#! /usr/bin/env python

from collections import defaultdict
import dbus.service
import dbus.glib
from dbus.mainloop.glib import DBusGMainLoop
import gobject

import logging


log=logging.getLogger("rebus.bus")
logging.basicConfig(level=1)


class REbus(dbus.service.Object):
    def __init__(self, bus, objpath):
        dbus.service.Object.__init__(self, bus, objpath)
        self.clients = {}
        self.domains = {}
        self.processed = defaultdict(set)
        self.descriptors = defaultdict(dict)

    @dbus.service.method(dbus_interface='com.airbus.rebus.bus',
                         in_signature='so', out_signature='',
                         sender_keyword='sender')                         
    def register(self, domain, pth, sender=None):
        self.domains[sender] = domain
        self.clients[sender] = pth
        log.info("New client %s (%s) in domain %s" % (pth, sender, domain))

    @dbus.service.method(dbus_interface='com.airbus.rebus.bus',
                         in_signature='s', out_signature='as',
                         sender_keyword='sender')
    def get_past_descriptors(self, selector, sender=None):
        return [ s
                 for s in self.selectors[sender].itervalues() 
                 if s.selector.startswith(selector) ]

    @dbus.service.method(dbus_interface='com.airbus.rebus.bus',
                         in_signature='ss', out_signature='',
                         sender_keyword='sender')
    def push(self, selector, descriptor, sender=None):
        domain = self.domains[sender]
        if selector not in self.descriptors[domain]:
            log.info("PUSH: %s => %s:%s" % (sender, domain, selector))
            self.descriptors[domain][selector] = descriptor
            self.new_descriptor(sender, domain, selector)

    @dbus.service.method(dbus_interface='com.airbus.rebus.bus',
                         in_signature='s', out_signature='s',
                         sender_keyword='sender')
    def get(self, selector, sender=None):
        domain = self.domains[sender]
        log.info("GET: %s %s:%s" % (sender, domain, selector))
        return self.descriptors[domain][selector]

    @dbus.service.method(dbus_interface='com.airbus.rebus.bus',
                         in_signature='s', out_signature='b',
                         sender_keyword='sender')
    def lock(self, selector, sender=None):
        domain = self.domains[sender]
        objpath = self.clients[sender]
        processed  = self.processed[domain]
        key = (objpath,selector)

        log.info("LOCK: %s(%s) => %r %s:%s "%(objpath, sender, key in processed, domain, selector))
        if key in processed:
            return False
        processed.add(key)
        return True

        
    @dbus.service.signal(dbus_interface='com.airbus.rebus.bus',
                         signature='sss')
    def new_descriptor(self, sender, domain, selector):
        pass


def main():
    gobject.threads_init()
    dbus.glib.init_threads()
    DBusGMainLoop(set_as_default=True)

    bus = dbus.SessionBus()    
    name = dbus.service.BusName("com.airbus.rebus.bus", bus)
    svc = REbus(bus, "/bus")
    

    mainloop = gobject.MainLoop()
    log.info("Entering main loop.")
    mainloop.run()


if __name__ == "__main__":
    main()
