import os
import re
import cPickle
from collections import defaultdict
from collections import OrderedDict
from collections import Counter
from rebus.storage import Storage
from rebus.descriptor import Descriptor
from rebus.tools.config import get_output_altering_options


@Storage.register
class DiskStorage(Storage):
    """
    Disk storage and retrieval of descriptor objects.
    """

    _name_ = "diskstorage"
    STORES_INTSTATE = True
    selector_regex = re.compile('^[a-zA-Z0-9~%/_-]*$')
    domain_regex = re.compile('^[a-zA-Z0-9-]*$')

    def __init__(self, **kwargs):
        self.basepath = kwargs['path'].rstrip('/')

        if not os.path.isdir(self.basepath):
            raise IOError('Directory %s does not exist' % self.basepath)
        if not os.path.isdir(self.basepath + '/agent_intstate'):
            os.makedirs(self.basepath + '/agent_intstate')

        #: Set of existing descriptor storage paths, all starting and ending
        #: with '/'
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
        self.processed = defaultdict(OrderedDict)

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
        self.discover('/')

    def discover(self, relpath):
        """
        Recursively add existing files to storage.

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
                self.discover(relname + '/')
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
                        desc = Descriptor.unserialize(fp.read())
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

                        self.register_meta(desc)
                elif name.endswith('.cfg') and relpath == '/':
                    # Bus configuration
                    # TODO periodically save this file. Use two file, overwrite
                    # oldest.
                    if elem == '_processed.cfg':
                        with open(name, 'rb') as fp:
                            # copy processed info to self.processed
                            p = cPickle.load(fp)
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

    def register_meta(self, desc):
        """
        :param desc: Descriptor instance
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
        self.uuids[domain][desc.uuid].add(selector)
        if not self.labels[domain][desc.uuid] or not desc.precursors:
            # Heuristic for choosing uuid label : prefer label of a descriptor
            # that has no precursor
            self.labels[domain][desc.uuid] = desc.label

    def find(self, domain, selector_regex, limit):
        regex = re.compile(selector_regex)
        sel_list = reversed(self.processed[domain].keys())
        res = []

        for k in sel_list:
            if regex.match(k):
                res.append(k)
                if limit != 0 and len(res) >= limit:
                    return res
        return res

    def find_by_selector(self, domain, selector_prefix, serialized=False):
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
                        open(path + name, 'rb').read())
                    selector = path[len(self.basepath)+len(domain)+1:] +\
                        name.split('.')[0]
                    if serialized:
                        desc = self.get_descriptor(domain, selector)
                        result.append(desc)
        return result

    def find_by_uuid(self, domain, uuid, serialized=False):
        result = []
        for selector in self.uuids[domain][uuid]:
            desc = self.get_descriptor(domain, selector)
            if serialized:
                result.append(desc)
        return result

    def find_by_value(self, domain, selector_prefix, value_regex,
                      serialized=False):
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
                        open(path + name, 'rb').read())
                    if re.match(value_regex, contents):
                        selector = path[len(self.basepath)+len(domain)+1:] +\
                            name.split('.')[0]
                        if serialized:
                            desc = self.get_descriptor(domain, selector)
                            result.append(desc)
        return result

    def list_uuids(self, domain):
        result = dict()
        for uuid in self.uuids[domain].keys():
            result[uuid] = self.labels[domain][uuid]
        return result

    def version_lookup(self, domain, selector):
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

    def get_descriptor(self, domain, selector, serialized=False):
        """
        Returns serialized descriptor metadata
        """
        # TODO remove serialized, always return serialized objects
        selector = self.version_lookup(domain, selector)
        if not selector:
            return "N."  # serialized None # TODO serialize None in bus

        fullpath = self.pathFromSelector(domain, selector) + ".meta"
        if not os.path.isfile(fullpath):
            return "N."  # serialized None # TODO serialize in bus
        return open(fullpath, "rb").read()

    def get_value(self, domain, selector, serialized):
        """
        Returns serialized descriptor value
        """
        none_value = "N." if serialized else None
        selector = self.version_lookup(domain, selector)
        if not selector:
            return none_value  # serialized None # TODO serialize None in bus

        fullpath = self.pathFromSelector(domain, selector) + ".value"
        if not os.path.isfile(fullpath):
            return none_value  # serialized None # TODO serialize in bus
        if serialized:
            return open(fullpath, "rb").read()
        else:
            return Descriptor.unserialize_value(open(fullpath, "rb").read())

    def get_children(self, domain, selector, serialized=False, recurse=True):
        """
        Always returns serialized descriptors
        """
        result = set()
        if selector not in self.processed[domain].keys():
            return result
        for child in self.edges[domain][selector]:
            result.add(self.get_descriptor(domain, child))
            if recurse:
                result |= self.get_children(child, domain, serialized, recurse)
        return result

    def mkdirs(self, domain, selector):
        """
        :param selector:  /sel/ector/%1234
        Create necessary directories, returns full path + file name for
        selector storage
        """
        fullpath, fname = self.pathFromSelector(domain, selector).split('%')
        if os.path.dirname(fullpath) + '/' not in self.existing_paths:
            os.makedirs(fullpath)
            self.existing_paths.add(fullpath)
        return fullpath + '%' + fname

    def pathFromSelector(self, domain, selector):
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

    def add(self, descriptor, serialized_descriptor=None):
        """
        serialized_descriptor is not used by this backend.
        """
        selector = descriptor.selector
        domain = descriptor.domain
        fname = self.mkdirs(domain, selector)
        if os.path.isfile(fname + '.meta'):
            # File already exists
            return False

        self.register_meta(descriptor)

        serialized_meta = descriptor.serialize_meta()
        serialized_value = descriptor.serialize_value()

        # Write meta
        with open(fname + '.meta', 'wb') as fp:
            fp.write(serialized_meta)

        # Write value
        with open(fname + '.value', 'wb') as fp:
            fp.write(serialized_value)

        self.processed[domain][selector] = set()
        return True

    def mark_processed(self, domain, selector, agent_name, config_txt):
        result = False
        key = (agent_name, config_txt)
        # Add to processed if not already there
        if key not in self.processed[domain][selector]:
            result = True
            self.processed[domain][selector].add(key)
        # Remove from processable
        if selector in self.processable[domain]:
            if key in self.processable[domain][selector]:
                result = False
                self.processable[domain][selector].discard(key)
        return result

    def mark_processable(self, domain, selector, agent_name, config_txt):
        result = False
        key = (agent_name, config_txt)
        if key not in self.processable[domain][selector]:
            self.processable[domain][selector].add((agent_name, config_txt))
            if key not in self.processed[domain][selector]:
                # avoid case where two instances of an agent run in different
                # modes
                result = True
        return result

    def get_processed(self, domain, selector):
        return self.processed[domain][selector]

    def get_processable(self, domain, selector):
        return self.processable[domain][selector]

    def processed_stats(self, domain):
        """
        Returns a list of couples, (agent names, number of processed selectors)
        and the total amount of selectors in this domain.
        """
        result = Counter()
        processed = self.processed[domain]
        for agentlist in processed.values():
            result.update([name for name, _ in agentlist])
        return result.items(), len(processed)

    def store_agent_state(self, agent_id, state):
        fname = os.path.join(self.basepath, 'agent_intstate', agent_id +
                             '.intstate')
        with open(fname, 'wb') as fp:
            fp.write(state)

    def load_agent_state(self, agent_id):
        fname = os.path.join(self.basepath, 'agent_intstate', agent_id +
                             '.intstate')
        if not os.path.isfile(fname):
            return ""
        with open(fname, 'rb') as fp:
            return fp.read()

    def store_state(self):
        with open(self.basepath + '/_processed.cfg', 'wb') as fp:
            cPickle.dump(self.processed, fp)

    def list_unprocessed_by_agent(self, agent_name, config_txt):
        res = []
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
            for sel, name_confs in self.processed[domain].iteritems():
                if agent_nameconf not in name_confs:
                    continue
                selectors.remove(sel)
            # selectors now contains a list of selectors that have not been
            # processed by this agent_nameconf

            for sel in selectors:
                for uuid in sel_to_uuid[sel]:
                    res.append((domain, uuid, sel))
        return res

    @staticmethod
    def add_arguments(subparser):
        subparser.add_argument("--path", help="Disk storage path (defaults to\
                               /tmp/rebus)",
                               default="/tmp/rebus")
