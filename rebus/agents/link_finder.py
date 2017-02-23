#! /usr/bin/env python

import hashlib
from collections import defaultdict
from rebus.agent import Agent


@Agent.register
class LinkFinder(Agent):
    _name_ = "link_finder"
    _desc_ = ("Find messages that are related and notify about it. "
              "Works in a single domain.")

    @classmethod
    def add_arguments(cls, subparser):
        subparser.add_argument(
            "-s", "--selector-prefix", default="",
            help="Only consider descriptors whose selector start with "
            "selector-prefix")

    def selector_filter(self, selector):
        if selector.startswith(self.prefix):
            return True

    def init_agent(self):
        self.memories = defaultdict(set)
        self.prefix = self.config['selector_prefix']
        # make sure all known descriptors are recorded in self.memories
        # useful in case the agent is re-started
        for desc in self.bus.find_by_selector(
                self.id, self.domain, self.prefix):
            pth, hsh = desc.selector.split('%', 1)
            val = self._calc_val(desc)
            key = (pth, val)
            self.memories[key].add(hsh)

    def process(self, descriptor, sender_id):
        sel = descriptor.selector
        if descriptor.domain != self.domain:
            return
        pth, hsh = sel.split('%', 1)
        val = self._calc_val(descriptor)
        key = (pth, val)
        if key in self.memories:
            related = self.memories[key]
            self.log.debug("%r related to %r", sel, related)
            for r in related:
                rel_sel = pth+'%'+r
                rel_desc = self.get(self.domain, rel_sel)
                linktype = pth.strip("/").replace("/", "-")
                self.declare_link(descriptor, rel_desc, linktype,
                                  "Same value on %s" % pth, isSymmetric=True)
        self.memories[key].add(hsh)

    def _calc_val(self, desc):
        try:
            if len(desc.value) < 200:
                return desc.value
            else:
                return hashlib.sha256(desc.value).digest()
        except TypeError:
            return hashlib.sha256(str(desc.value)).digest()
