import logging
import os
import re
import threading
import time
from collections import defaultdict
from collections import OrderedDict
from collections import Counter
from rebus.storage import Storage
from rebus.descriptor import Descriptor
from rebus.tools.serializer import picklev2 as store_serializer
log = logging.getLogger("rebus.storage.diskstorage")


class CheckpointThread(threading.Thread):
    """
    Calls store_state periodically to avoid losing it completely in case
    diskstorage gets killed ungracefully.
    """
    def __init__(self, storage):
        threading.Thread.__init__(self)
        self.storage = storage

    def run(self):
        while True:
            time.sleep(5)
            self.storage.store_state()


@Storage.register
class DiskStorage(Storage):
    """
    Disk storage and retrieval of descriptor objects.
    """

    _name_ = "diskstorage"
    STORES_INTSTATE = True
    selector_regex = re.compile('^[a-zA-Z0-9~%/_-]*$')
    domain_regex = re.compile('^[a-zA-Z0-9-]*$')

    def __init__(self, options):
        self.basepath = options.path.rstrip('/')

        if not os.path.isdir(self.basepath):
            raise IOError('Directory %s does not exist' % self.basepath)
        if not os.path.isdir(self.basepath + '/agent_intstate'):
            os.makedirs(self.basepath + '/agent_intstate')

        #: Set of existing descriptor storage directories, all starting and
        #: ending with '/'
        self.existing_paths = set((self.basepath + '/',))

        #: self.version_cache['domain']['/selector/'][version] = /selector/%123
        #: where 1234 is the hash of this selector's version 42
        #: Mostly useful for descriptors that have several versions
        self.version_cache = defaultdict(lambda: defaultdict(dict))

        #: self.edges['domain']['selectorA'] is a set of selectors of
        #: descriptors that were spawned from selectorA.
        self.edges = defaultdict(lambda: defaultdict(set))

        #: self.processed['domain']['/selector/%hash'] is a set of (agent name,
        #: configuration text) that have finished processing, or declined to
        #: process this descriptor. Allows stopping and resuming the bus when
        #: not all descriptors have been processed.
        #: Order is kept for find requests & co-maintainability of
        #: {RAM,Disk}storage implementations.
        #: access to self.processed must be protected using self.processedlock
        self.processed = defaultdict(OrderedDict)

        self.unsavedprocessed = False

        #: protects access to self.processed
        self.processedlock = threading.RLock()

        #: self.processable['domain']['/selector/%hash'] is a set of (agent
        #: name, configuration text) that are running in interactive mode, and
        #: are able to process this descriptor.
        #: This attribute is not saved to disk.
        self.processable = defaultdict(lambda: defaultdict(set))

        #: self.uuids['domain']['uuid'] is the set of selectors that belong to
        #: descriptors having this uuid
        self.uuids = defaultdict(lambda: defaultdict(set))

        #: self.labels['domain']['uuid'] is the label of descriptors having
        #: this UUID
        self.labels = defaultdict(lambda: defaultdict(str))

        # Enumerate existing files & dirs
        with self.processedlock:
            self._discover('/')

        # start _processed flushing thread
        self.checkpointThread = CheckpointThread(self)
        self.checkpointThread.daemon = True
        self.checkpointThread.start()

    def _discover(self, relpath):
        """
        Recursively add existing files to storage.

        self.processedlock must be acquired prior to calling this function

        :param relpath: starts and ends with a '/', relative to self.basepath
        """
        if relpath == '/agent_intstate/':
            # Ignore internal state of agents
            return

        path = self.basepath + relpath
        self.existing_paths.add(path)

        for elem in os.listdir(path):
            name = path + elem
            relname = relpath + elem
            if os.path.isdir(name):
                self._discover(relname + '/')
            elif os.path.isfile(name):
                basename = name.rsplit('.', 1)[0]
                if name.endswith('.value'):
                    # Serialized descriptor value
                    if not os.path.isfile(basename + '.meta'):
                        raise Exception(
                            'Missing associated metadata for %s' % relname)
                elif name.endswith('.meta'):
                    # Serialized descriptor metadata
                    if not os.path.isfile(basename + '.value'):
                        raise Exception(
                            'Missing associated value for %s' % relname)
                    with open(name, 'rb') as fp:
                        try:
                            desc = Descriptor.unserialize(store_serializer,
                                                          fp.read())
                        except:
                            log.error(
                                "Could not unserialize metadata from file %s",
                                name)
                            raise
                        fname_selector = relname.rsplit('.')[0]
                        # check consistency between file name and serialized
                        # metadata
                        fname_domain = fname_selector.split('/')[1]
                        if fname_domain != desc.domain:
                            raise Exception(
                                'Filename domain %s does not match metadata '
                                'domain %s for descriptor %s' %
                                (fname_domain, desc.domain, fname_selector))
                        fname_hash = fname_selector.rsplit('%', 1)[1]
                        if fname_hash != desc.hash:
                            raise Exception(
                                'Filename hash %s does not match metadata hash'
                                ' %s for descriptor %s' %
                                (fname_hash, desc.domain, fname_selector))

                        self._register_meta(desc)
                elif name.endswith('.cfg') and relpath == '/':
                    # Bus configuration
                    # TODO periodically save this file. Use two file, overwrite
                    # oldest.
                    if elem == '_processed.cfg':
                        with open(name, 'rb') as fp:
                            # copy processed info to self.processed
                            p = store_serializer.load(fp)
                            for dom in p.keys():
                                for sel, val in p[dom].items():
                                    self.processed[dom][sel] = val
                else:
                    raise Exception(
                        'Invalid file name - %s has an invalid extension '
                        '(must be .value, .meta or .cfg)' % relname)
            else:
                raise Exception(
                    'Invalid file type - %s is neither a regular file nor a '
                    'directory' % name)

    def _register_meta(self, desc):
        """
        :param desc: Descriptor instance
        self.processedlock must be acquired prior to calling this function
        """

        domain = desc.domain
        selector = desc.selector
        self.version_cache[domain][selector.split('%')[0]][desc.version]\
            = selector
        for precursor in desc.precursors:
            self.edges[domain][precursor].add(selector)
        if selector not in self.processed[domain]:
            # If it has not been restored from processed.cfg
            self.processed[domain][selector] = set()
            self.unsavedprocessed = True
        self.uuids[domain][desc.uuid].add(selector)
        if not self.labels[domain][desc.uuid] or not desc.precursors:
            # Heuristic for choosing uuid label : prefer label of a descriptor
            # that has no precursor
            self.labels[domain][desc.uuid] = desc.label

    def find(self, domain, selector_regex, limit=0, offset=0):
        regex = re.compile(selector_regex)
        sel_list = reversed(self.processed[domain].keys())
        result = []

        for k in sel_list:
            if regex.match(k):
                if offset > 0:
                    offset -= 1
                    continue
                result.append(k)
                if limit != 0 and len(result) >= limit:
                    return result
        return result

    def find_by_selector(self, domain, selector_prefix, limit=0, offset=0):
        result = []
        for selector in self.processed[domain].keys():
            if selector.startswith(selector_prefix):
                if offset > 0:
                    offset -= 1
                    continue
                desc = self.get_descriptor(domain, selector)
                if desc:
                    result.append(desc)
                if limit != 0 and len(result) >= limit:
                    return result
        return result

    def find_by_uuid(self, domain, uuid):
        result = []
        for selector in self.uuids[domain][uuid]:
            desc = self.get_descriptor(domain, selector)
            if desc is None:
                # that would be a bug
                log.warning(
                    "Descriptor %s:%s could not be retrieved in "
                    "find_by_uuid (%s)", domain, selector, uuid)
            else:
                result.append(desc)
        return result

    def find_by_value(self, domain, selector_prefix, value_regex):
        result = []
        # File paths to explore
        pathprefix = self.basepath + '/' + domain + selector_prefix
        paths = [path for path in self.existing_paths if
                 path.startswith(pathprefix)]
        for path in paths:
            # open and run re.match() on every file matching *.value
            for name in os.listdir(path):
                if os.path.isfile(path + name) and name.endswith('.value'):
                    contents = Descriptor.unserialize_value(
                        store_serializer,
                        open(path + name, 'rb').read())
                    if re.match(value_regex, contents):
                        selector = path[len(self.basepath)+len(domain)+1:] +\
                            name.split('.')[0]
                        desc = self.get_descriptor(domain, selector)
                        if desc:
                            result.append(desc)
        return result

    def list_uuids(self, domain):
        result = dict()
        for uuid in self.uuids[domain].keys():
            result[uuid] = self.labels[domain][uuid]
        return result

    def _version_lookup(self, domain, selector):
        """
        :param selector: selector, containing either a version (/selector/~12)
        or a hash (/selector/%1234)
        Perform version lookup if needed.
        Returns a selector containing a hash value /selector/%1234
        """
        # if version is specified, but no hash
        if '%' not in selector and '~' in selector:
            selprefix, version = selector.split('~')
            try:
                intversion = int(version)
                if intversion < 0:
                    maxversion = max(self.version_cache[domain][selprefix])
                    intversion = maxversion + intversion + 1
                selector = self.version_cache[domain][selprefix][intversion]
            except (KeyError, ValueError):
                # ValueError: invalid version integer
                # KeyError: unknown version
                selector = None
        return selector

    def get_descriptor(self, domain, selector):
        """
        Returns descriptor metadata, None if descriptor was not found.
        """
        selector = self._version_lookup(domain, selector)
        if not selector:
            return None

        fullpath = self._pathFromSelector(domain, selector) + ".meta"
        if not os.path.isfile(fullpath):
            return None
        return Descriptor.unserialize(store_serializer,
                                      open(fullpath, "rb").read())

    def get_value(self, domain, selector):
        """
        Returns descriptor value, None if descriptor was not found.
        """
        selector = self._version_lookup(domain, selector)
        if not selector:
            return None

        fullpath = self._pathFromSelector(domain, selector) + ".value"
        if not os.path.isfile(fullpath):
            return None
        try:
            value = Descriptor.unserialize_value(store_serializer,
                                                 open(fullpath, "rb").read())
        except:
            log.error("Could not unserialize value from file %s", fullpath)
            raise
        return value

    def get_children(self, domain, selector, recurse=True):
        result = set()
        with self.processedlock:
            if selector not in self.processed[domain].keys():
                return result
        for child in self.edges[domain][selector]:
            desc = self.get_descriptor(domain, child)
            if desc:
                result.add(desc)
            if recurse:
                result |= self.get_children(child, domain, recurse)
        return result

    def _mkdirs(self, domain, selector):
        """
        :param selector:  /sel/ector/%1234
        Create necessary directories, returns full path + file name for
        selector storage
        """
        fullpath, fname = self._pathFromSelector(domain, selector).split('%')
        # make sure fullpath ends with one "/"
        fullpath = os.path.dirname(fullpath + '/') + '/'
        if fullpath not in self.existing_paths:
            try:
                os.makedirs(fullpath)
            except OSError as e:
                if e.args[0] != 17:  # File exists
                    raise
            self.existing_paths.add(fullpath)
        return fullpath + '%' + fname

    def _pathFromSelector(self, domain, selector):
        """
        Returns full path, from domain and selector
        Checks selector & domain sanity (character whitelist)
        """
        if not self.selector_regex.match(selector):
            raise Exception("Provided selector (hex: %s) contains forbidden "
                            "characters" % selector.encode('hex'))
        if not self.domain_regex.match(domain):
            raise Exception("Provided domain (hex: %s) contains forbidden "
                            "characters" % domain.encode('hex'))

        path = os.path.join(self.basepath, domain, selector[1:])
        return path

    def add(self, descriptor):
        """
        serialized_descriptor is not used by this backend.
        """
        selector = descriptor.selector
        domain = descriptor.domain
        fname = self._mkdirs(domain, selector)
        if os.path.isfile(fname + '.meta'):
            # File already exists
            return False

        self._register_meta(descriptor)

        serialized_meta = descriptor.serialize_meta(store_serializer)
        serialized_value = descriptor.serialize_value(store_serializer)

        # Write meta
        with open(fname + '.meta', 'wb') as fp:
            fp.write(serialized_meta)

        # Write value
        with open(fname + '.value', 'wb') as fp:
            fp.write(serialized_value)

        with self.processedlock:
            self.processed[domain][selector] = set()
            self.unsavedprocessed = True
        return True

    def mark_processed(self, domain, selector, agent_name, config_txt):
        result = False
        key = (agent_name, config_txt)
        # Add to processed if not already there
        with self.processedlock:
            if key not in self.processed[domain][selector]:
                result = True
                self.processed[domain][selector].add(key)
                self.unsavedprocessed = True
        # Remove from processable
        if selector in self.processable[domain]:
            if key in self.processable[domain][selector]:
                result = False
                self.processable[domain][selector].discard(key)
        return result

    def mark_processable(self, domain, selector, agent_name, config_txt):
        result = False
        key = (agent_name, config_txt)
        with self.processedlock:
            if key not in self.processable[domain][selector]:
                self.processable[domain][selector].add((agent_name,
                                                        config_txt))
                if key not in self.processed[domain][selector]:
                    # avoid case where two instances of an agent run in
                    # different modes
                    result = True
        return result

    def get_processed(self, domain, selector):
        with self.processedlock:
            return self.processed[domain][selector]

    def get_processable(self, domain, selector):
        return self.processable[domain][selector]

    def processed_stats(self, domain):
        """
        Returns a list of couples, (agent names, number of processed selectors)
        and the total amount of selectors in this domain.
        """
        result = Counter()
        with self.processedlock:
            processed = self.processed[domain]
        for agentlist in processed.values():
            result.update([name for name, _ in agentlist])
        return result.items(), len(processed)

    def store_agent_state(self, agent_name, state):
        fname = os.path.join(self.basepath, 'agent_intstate', agent_name +
                             '.intstate')
        with open(fname, 'wb') as fp:
            fp.write(state)

    def load_agent_state(self, agent_name):
        fname = os.path.join(self.basepath, 'agent_intstate', agent_name +
                             '.intstate')
        if not os.path.isfile(fname):
            return ""
        with open(fname, 'rb') as fp:
            return fp.read()

    def store_state(self):
        if self.unsavedprocessed:
            with self.processedlock:
                with open(self.basepath + '/_processed.cfg', 'wb') as fp:
                    store_serializer.dump(self.processed, fp)
                self.unsavedprocessed = False

    def list_unprocessed_by_agent(self, agent_name, config_txt):
        result = []
        agent_nameconf = (agent_name, config_txt)
        for domain in self.version_cache.keys():

            # list all selectors
            selectors = set()

            # build temporary selector to uuids lookup table
            # 1 selectors might belong in several uuids (or might in the
            # future), add them all
            sel_to_uuid = defaultdict(list)
            for uuid, selset in self.uuids[domain].iteritems():
                selectors.update(selset)
                for sss in selset:
                    sel_to_uuid[sss].append(uuid)
            with self.processedlock:
                for sel, name_confs in self.processed[domain].iteritems():
                    if agent_nameconf not in name_confs:
                        continue
                    try:
                        selectors.remove(sel)
                    except KeyError:
                        # happens when files are deleted from diskstorage, then
                        # bus is restarted
                        log.warning(
                            "Selector %s is mentioned in 'processed', but has "
                            "not been registered", sel)
            # selectors now contains a list of selectors that have not been
            # processed by this agent_nameconf

            for sel in selectors:
                for uuid in sel_to_uuid[sel]:
                    result.append((domain, uuid, sel))
        return result

    @staticmethod
    def add_arguments(subparser):
        subparser.add_argument(
            "--path", help="Disk storage path (defaults to /tmp/rebus)",
            default="/tmp/rebus")
