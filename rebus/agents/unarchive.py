#! /usr/bin/env python

from rebus.agent import Agent
from rebus.descriptor import Descriptor
from rebus.agents.inject import guess_selector
from StringIO import StringIO
from tempfile import mkdtemp, NamedTemporaryFile
import distutils.spawn
import os
import os.path
import shutil
import subprocess


@Agent.register
class Unarchive(Agent):
    _name_ = "unarchive"
    _desc_ = "Extract archives and uncompress files"
    _operationmodes_ = ('automatic', 'interactive')

    def init_agent(self):
        self.cabextract = distutils.spawn.find_executable("cabextract")
        if self.cabextract is None:
            self.log.warning("cabextract executable not found - cab archives "
                             "will not be extracted")

    def selector_filter(self, selector):
        return selector.startswith("/archive/") or\
            selector.startswith("/compressed/")

    def process(self, descriptor, sender_id):
        import tarfile
        data = descriptor.value
        selector = descriptor.selector

        #: List of (unarchived file name, descriptor label, unarchived file
        #: contents)
        unarchived = []

        def do_untar(archive, mode, archive_label=descriptor.label,
                     unarchived=unarchived):
            tar = tarfile.open(fileobj=StringIO(archive), mode=mode)
            for finfo in tar.getmembers():
                if finfo.isfile() and finfo.size > 0:
                    fname = os.path.basename(finfo.name)
                    unarchived.append((fname, archive_label + ":" + fname,
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
                fname = descriptor.label
                if fname.endswith('.bz2'):
                    fname = fname[:-4]
                else:
                    fname = "bunzipped %s" % fname
                unarchived.append((fname, fname, data))
        if "/compressed/gzip" in selector:
            # Try and extract - might be a .tar.gz
            try:
                do_untar(descriptor.value, "r:gz")
            except tarfile.TarError:
                # Probably not a compressed tar file
                from gzip import GzipFile
                data = GzipFile(fileobj=StringIO(descriptor.value),
                                mode='rb').read()
                fname = descriptor.label
                if fname.endswith('.gz'):
                    fname = fname[:-3]
                else:
                    fname = "gunzipped %s" % fname
                unarchived.append((fname, fname, data))

        # Archive files
        if "/archive/tar" in selector:
            do_untar(descriptor.value, mode=None)
        if "/archive/zip" in selector:
            from zipfile import ZipFile
            fzip = ZipFile(file=StringIO(descriptor.value))
            for fname in fzip.namelist():
                unarchived.append((fname, descriptor.label + ':' + fname,
                                   fzip.read(fname)))

        if "/archive/cab" in selector and self.cabextract:
            try:
                tmpdir = mkdtemp("rebus-cabextract")
                with NamedTemporaryFile(prefix="rebus-cab") as cabfile:
                    cabfile.write(descriptor.value)
                    cabfile.flush()
                    try:
                        subprocess.check_output([self.cabextract, '-d', tmpdir,
                                                 cabfile.name],
                                                stderr=subprocess.STDOUT)
                    except subprocess.CalledProcessError as e:
                        self.log.error("cabextract exited with status %d" %
                                       e.returncode)
                        self.log.error(e.output)
                for fname in os.listdir(tmpdir):
                    filepathname = os.path.join(tmpdir, fname)
                    unarchived.append((fname, descriptor.label + ':' + fname,
                                       open(filepathname, 'rb').read()))
            finally:
                shutil.rmtree(tmpdir)

        for fname, desclabel, fcontents in unarchived:
            selector = guess_selector(buf=fcontents)
            desc = Descriptor(desclabel, selector, fcontents,
                              descriptor.domain, agent=self._name_)
            self.push(desc)
            self.declare_link(
                descriptor, desc, "Unarchived", "\"%s\" has been unarchived "
                "from \"%s\"" % (fname, descriptor.label))
