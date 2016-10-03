#! /usr/bin/env python

import os
import time
import uuid
from rebus.agent import Agent
from rebus.descriptor import Descriptor
from rebus.tools.selectors import guess_selector


@Agent.register
class Inject(Agent):
    _name_ = "inject"
    _desc_ = "Inject files into the bus"
    _operationmodes_ = ('automatic', )

    @classmethod
    def add_arguments(cls, subparser):
        subparser.add_argument("files", nargs="+",
                               help="Inject FILES into the bus")
        subparser.add_argument("--selector", "-s",
                               help="Use SELECTOR")
        subparser.add_argument("--uuid", type=lambda x: str(uuid.UUID(x)),
                               help="Override UUID")
        subparser.add_argument("--label", "-l",
                               help="Use LABEL instead of file name")
        subparser.add_argument("--printable", '-p', action='store_true',
                               help="Mark this value as printable. Use if the "
                               "raw value may be displayed to an analyst.")

    def run(self):
        dparam = ({} if not self.config["uuid"]
                  else {"uuid": self.config["uuid"]})
        for f in self.config['files']:
            start = time.time()
            label = self.config['label'] if self.config['label'] else \
                os.path.basename(f)
            try:
                data = open(f).read()
            except IOError as e:
                if e.errno != os.errno.ENOENT:
                    raise
                self.log.warning("File [%s] not found" % f)
                continue
            if self.config['printable']:
                data = unicode(data)

            selector = self.config['selector'] if self.config['selector'] \
                else guess_selector(buf=data, label=label)
            done = time.time()
            desc = Descriptor(label, selector, data, self.domain,
                              agent=self._name_, processing_time=(done-start),
                              **dparam)
            self.push(desc)
