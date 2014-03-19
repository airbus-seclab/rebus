#! /usr/bin/env python

import sys
import os
from rebus.agent import Agent
from rebus.descriptor import Descriptor
import subprocess
import logging


def guess_selector(fname):
    guess = subprocess.check_output(["file", "-L", fname])
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
            selector =  options.selector if options.selector else  guess_selector(f)
            desc = Descriptor(label, selector, data, options.domain)
            self.push(desc)
