#! /usr/bin/env python

import os
import time
from rebus.agent import Agent
from rebus.descriptor import Descriptor
from rebus.agents.inject import guess_selector
from StringIO import StringIO



@Agent.register
class Unarchive(Agent):
    _name_ = "unarchive"
    _desc_ = "Extract archives and uncompress files"

    def selector_filter(self, selector):
        return selector.startswith("/archive/") or\
            selector.startswith("/compressed/")

    def process(self, descriptor, sender_id):
        import tarfile
        start = time.time()
        data = descriptor.value
        selector = descriptor.selector

        #: List of (unarchived file name, unarchived file contents)
        unarchived = []

        def do_untar(archive, mode, archive_label=descriptor.label, unarchived=unarchived):
            tar = tarfile.open(fileobj=StringIO(archive), mode=mode)
            for finfo in tar.getmembers():
                if finfo.isfile() and finfo.size > 0:
                    fname = os.path.basename(finfo.name)
                    unarchived.append((archive_label + ":" + fname,
                                       tar.extractfile(finfo).read()))


        # Compressed files
        if "/compressed/bzip2" in selector:
            # Try and extract - might be a .tar.bz2
            try:
                do_untar(descriptor.value, "r:bz2")
            except tarfile.TarError:
                # Probably not a compressed tar file
                import bz2
                data = bz2.decompress(descriptor.value)
                unarchived.append(("bunzipped %s" % descriptor.label, data))
        if "/compressed/gzip" in selector:
            # Try and extract - might be a .tar.gz
            try:
                do_untar(descriptor.value, "r:gz")
            except tarfile.TarError:
                # Probably not a compressed tar file
                from gzip import GzipFile
                data = GzipFile(fileobj=StringIO(descriptor.value),
                                mode='rb').read()
                unarchived.append(("gunzipped %s" % descriptor.label, data))

        # Archive files
        if "/archive/tar" in selector:
            do_untar(descriptor.value, mode=None)
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
