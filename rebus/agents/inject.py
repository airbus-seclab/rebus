#! /usr/bin/env python

import os
from rebus.agent import Agent
from rebus.descriptor import Descriptor
import magic


def guess_selector(fname=None, buf=None):
    if fname is not None:
        guess = magic.from_file(fname)
    elif buf is not None:
        guess = magic.from_buffer(buf)
    else:
        raise Exception("Either fname or buffer must be set when calling "
                        "guess_selector.")
    if "ELF" in guess:
        return "/binary/elf"
    if "PE" in guess:
        return "/binary/pe"
    if "DOS" in guess:
        return "/binary/dos"
    if "Mach-O" in guess:
        return "/binary/macho"
    return "/unknown"


@Agent.register
class Inject(Agent):
    _name_ = "inject"
    _desc_ = "Inject files into the bus"

    @classmethod
    def add_arguments(cls, subparser):
        subparser.add_argument("files", nargs="+",
                               help="Inject FILES into the bus")
        subparser.add_argument("--selector", "-s",
                               help="Use SELECTOR")
        subparser.add_argument("--label", "-l",
                               help="Use LABEL instead of file name")

    def run(self, options):
        for f in options.files:
            label = options.label if options.label else os.path.basename(f)
            data = open(f).read()
            selector = options.selector if options.selector else \
                guess_selector(fname=f)
            desc = Descriptor(label, selector, data, options.domain)
            self.push(desc)
