#! /usr/bin/env python

import os
import hashlib
from collections import defaultdict
from rebus.agent import Agent
from rebus.descriptor import Descriptor

@Agent.register
class LinkFinder(Agent):
    _name_ = "link_finder"
    _desc_ = "Find messages that are related and notify about it"

    def __init__(self, bus, name=None, domain='default'):
        Agent.__init__(self, bus, name=name, domain=domain)
        self.memories = defaultdict(list)
        
    def process(self, descriptor, sender_id):
        sel = descriptor.selector
        p = sel.find("%")
        pth,hsh = sel[:p],sel[p:]
        try:
            val = hashlib.md5(descriptor.value).digest()
        except TypeError:
            val = hashlib.md5(str(descriptor.value)).digest()
        key = (pth,val)
        if key in self.memories:
            related = self.memories[key]
            self.log.debug("%r related to %r" % (sel, related))
            for r in related:
                rel_sel = pth+r
                rel_desc = self.get(self.domain, rel_sel)
                self.declare_link(descriptor, rel_desc, pth, "Same value on %s" % pth)
        self.memories[key].append(hsh)

