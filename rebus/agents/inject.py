#! /usr/bin/env python

import os
from rebus.agent import Agent
from rebus.descriptor import Descriptor
from rebus.tools import magic_wrap
import struct
import time
import uuid


def guess_selector(fname=None, buf=None, label=None):
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
    if "Microsoft Cabinet archive data" in guess:
        return "/archive/cab"

    # E-mails
    if 'Composite Document File V2 Document' in guess:
        if label.endswith('.msg'):
            return "/email/msg"
    if 'ASCII text' in guess:
        if label.endswith('.eml'):
            return '/email/eml'
        # TODO peek at contents & grep headers to identify e-mail?
    if 'RFC 822 mail, ASCII text' in guess:
        return '/email/eml'
    if 'SMTP mail, ASCII text' in guess:
        return '/email/eml'
    return "/unknown"


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
