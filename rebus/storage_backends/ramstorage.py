from rebus.storage import Storage
import re
from collections import defaultdict
from collections import OrderedDict
from collections import Counter
from rebus.tools.config import get_output_altering_options


@Storage.register
class RAMStorage(Storage):
    """
    RAM storage and retrieval of descriptor objects.
    """

    _name_ = "ramstorage"
    STORES_INTSTATE = False

    def __init__(self, **kwargs):
        #: self.dstore['domain']['/selector/%hash'] is a descriptor
        self.dstore = defaultdict(OrderedDict)

        #: self.serialized_store['domain']['/selector/%hash'] is a serialized
        #: descriptor
        self.serialized_store = defaultdict(dict)

        #: self.version_cache['domain']['/selector/'][42] = /selector/%1234
        #: where 1234 is the hash of this selector's version 42
        self.version_cache = defaultdict(lambda: defaultdict(dict))

        #: self.edges['domain']['selectorA'] is a set of selectors of
        #: descriptors that were spawned from selectorA.
        self.edges = defaultdict(lambda: defaultdict(set))

        #: self.processed['domain']['/selector/%hash'] is a set of (agent name,
        #: configuration text) that have finished processing, or declined to
        #: process this descriptor.
        #: Order is kept for find requests & co-maintainability of
        #: {RAM,Disk}storage implementations.
        self.processed = defaultdict(OrderedDict)

        #: self.processable['domain']['/selector/%hash'] is a set of (agent
        #: name, configuration text) that are running in interactive mode, and
        #: are able to process this descriptor.
        self.processable = defaultdict(lambda: defaultdict(set))

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

    def find_by_uuid(self, domain, uuid, serialized=False):
        result = []
        for selector, desc in self.dstore[domain].iteritems():
            if desc.uuid == uuid:
                if serialized:
                    result.append(self.serialized_store[domain][selector])
                else:
                    result.append(desc)
        return result

    def find_by_value(self, domain, selector_prefix, value_regex,
                      serialized=False):
        result = []
        for selector, desc in self.dstore[domain].iteritems():
            if desc.selector.startswith(selector_prefix) and \
                    re.match(value_regex, desc.value):
                if serialized:
                    result.append(self.serialized_store[domain][selector])
                else:
                    result.append(desc)
        return result

    def list_uuids(self, domain):
        result = dict()
        for desc in self.dstore[domain].values():
            result[desc.uuid] = desc.label
        return result

    def get_descriptor(self, domain, selector, serialized=False):
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

        # Check whether domain & selector are known
        if domain not in self.dstore or selector not in self.dstore[domain]:
            if serialized:
                return "N."  # serialized None
            else:
                return None
        if not serialized:
            return self.dstore[domain][selector]
        else:
            desc = self.serialized_store[domain][selector]
            if desc:
                return desc
            return self.dstore[domain][selector].serialize()

    def get_children(self, domain, selector, serialized=False, recurse=True):
        result = set()
        if selector not in self.dstore[domain]:
            return result
        for child in self.edges[domain][selector]:
            if serialized:
                result.add(self.serialized_store[domain][child])
            else:
                result.add(self.dstore[domain][child])
            if recurse:
                result |= self.get_children(child, domain, serialized, recurse)
        return result

    def add(self, descriptor, serialized_descriptor=None):
        selector = descriptor.selector
        domain = descriptor.domain
        if selector in self.dstore[domain]:
            return False
        if serialized_descriptor is not None:
            self.serialized_store[domain][selector] = serialized_descriptor
        self.dstore[domain][selector] = descriptor
        self.version_cache[domain][selector.split('%')[0]][descriptor.version]\
            = selector
        for precursor in descriptor.precursors:
            self.edges[domain][precursor].add(selector)
        self.processed[domain][selector] = set()
        return True

    def mark_processed(self, domain, selector, agent_name, config_txt):
        filtered_conf = get_output_altering_options(config_txt)
        self.processed[domain][selector].add((agent_name, filtered_conf))
        # Remove from processable
        if selector in self.processable[domain]:
            self.processable[domain][selector].discard((agent_name,
                                                        filtered_conf))

    def mark_processable(self, domain, selector, agent_name, config_txt):
        filtered_conf = get_output_altering_options(config_txt)
        self.processable[domain][selector].add((agent_name, filtered_conf))

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

    def list_unprocessed_by_agent(self, agent_name, config_txt):
        filtered_conf = get_output_altering_options(config_txt)
        res = []
        for domain in self.dstore.keys():
            selectors = set(self.dstore[domain].keys())
            processed_selectors = set([sel for sel, name_confs in
                                       self.processed[domain].items() if
                                       (agent_name, filtered_conf) in
                                       name_confs])
            unprocessed_sels = selectors - processed_selectors
            res.extend([(domain, sel) for sel in unprocessed_sels])
        return res
