#! /usr/bin/env python

import os
from rebus.agent import Agent
from rebus.descriptor import Descriptor
from rebus.tools import magic_wrap
import struct
import time


def guess_selector(fname=None, buf=None):
    if fname is not None:
        guess = magic_wrap.from_file(fname)
    elif buf is not None:
        guess = magic_wrap.from_buffer(buf)
    else:
        raise Exception("Either fname or buffer must be set when calling "
                        "guess_selector.")
    if ".Net" in guess:
        return "/binary/net"
    if "ELF" in guess:
        return "/binary/elf"
    if "PE" in guess:
        return "/binary/pe"
    if "DOS" in guess:
        # libmagic is a bit buggy
        # make sure it's not a PE
        try:
            if fname is not None:
                buf = open(fname).read()
            if buf is not None:
                # MZ.e_lfanew
                e_lfanew = struct.unpack('<I', buf[0x3C:0x3C+4])[0]
                if buf[e_lfanew:e_lfanew+4] == "PE\x00\x00":
                    return "/binary/pe"
        except:
            return "/binary/dos"
        return "/binary/dos"
    if "Mach-O" in guess:
        return "/binary/macho"

    # Compressed files
    if "gzip compressed data" in guess:
        return "/compressed/gzip"
    if "bzip2 compressed data" in guess:
        return "/compressed/bzip2"

    # Archive files
    if "POSIX tar archive" in guess:
        return "/archive/tar"
    if "Zip archive data" in guess:
        return "/archive/zip"
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

    def run(self):
        for f in self.options.files:
            start = time.time()
            label = self.options.label if self.options.label else \
                os.path.basename(f)
            data = open(f).read()
            selector = self.options.selector if self.options.selector else \
                guess_selector(buf=data)
            done = time.time()
            desc = Descriptor(label, selector, data, self.domain,
                              agent=self._name_, processing_time=(done-start))
            self.push(desc)
