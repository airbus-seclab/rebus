#! /usr/bin/env python

from rebus.agent import Agent
from rebus.descriptor import Descriptor
from rebus.agents.inject import guess_selector
import time
from StringIO import StringIO


@Agent.register
class Unarchive(Agent):
    _name_ = "unarchive"
    _desc_ = "Extract archives and uncompress files"

    def selector_filter(self, selector):
        return selector.startswith("/archive/") or\
            selector.startswith("/compressed/")

    def process(self, descriptor, sender_id):
        start = time.time()
        data = descriptor.value
        selector = descriptor.selector

        #: List of (unarchived file name, unarchived file contents)
        unarchived = []
        # Compressed files
        if "/compressed/bzip2" in selector:
            import bz2
            data = bz2.decompress(descriptor.value)
            unarchived.append(("bunzipped %s" % descriptor.label, data))
        if "/compressed/gzip" in selector:
            from gzip import GzipFile
            data = GzipFile(fileobj=StringIO(descriptor.value),
                            mode='rb').read()
            unarchived.append(("gunzipped %s" % descriptor.label, data))

        # Archive files
        if "/archive/tar" in selector:
            import tarfile
            tar = tarfile.open(fileobj=StringIO(descriptor.value))
            for fname in tar.getnames():
                unarchived.append((descriptor.label + ":" + fname,
                                   tar.extractfile(fname).read()))
        if "/archive/zip" in selector:
            from zipfile import ZipFile
            fzip = ZipFile(file=StringIO(descriptor.value))
            for fname in fzip.namelist():
                unarchived.append((descriptor.label + ':' + fname,
                                   fzip.read(fname)))

        for fname, fcontents in unarchived:
            selector = guess_selector(buf=fcontents)
            done = time.time()
            desc = Descriptor(fname, selector, fcontents, descriptor.domain,
                              agent=self._name_, processing_time=(done-start))
            self.push(desc)
            self.declare_link(descriptor, desc, "Unarchived", "%s has been \
                              unarchived from %s" % (fname, descriptor.label))
