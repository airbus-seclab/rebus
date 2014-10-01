#!/usr/bin/env python2

import re
from collections import defaultdict
from collections import OrderedDict


class DescriptorStorage(object):
    """
    Storage and retrieval of descriptor objects.
    """
    def __init__(self):
        # self.dstore['domain']['/selector/%hash'] is a serialized descriptor
        self.dstore = defaultdict(OrderedDict)
        self.serialized_store = defaultdict(dict)

        # self.version_cache['domain']['/selector/'][42] = /selector/%1234
        # where 1234 is the hash of this selector's version 42
        self.version_cache = defaultdict(lambda: defaultdict(dict))

        # self.edges['domain']['selectorA'] is a set of selectors of
        # descriptors that were spawned from selectorA.
        self.edges = defaultdict(lambda: defaultdict(set))

    def find(self, domain, selector_regex, limit):
        """
        Return array of selectors according to search constraints :
        * domain
        * selector regular expression
        * limit (max number of entries to return)
        """
        regex = re.compile(selector_regex)
        store = self.dstore[domain]
        res = []

        # FIXME : be more efficient ?
        for k in reversed(store.keys()):
            if regex.match(k):
                res.append(k)
                if limit != 0 and len(res) >= limit:
                    return res
        return res

    def find_by_uuid(self, domain, uuid, serialized=False):
        """
        Return a list of descriptors whose uuid match given parameter
        """
        result = []
        for selector, desc in self.dstore[domain].iteritems():
            if desc.uuid == uuid:
                if serialized:
                    result.append(self.serialized_store[domain][selector])
                else:
                    result.append(desc)
        return result

    def get_descriptor(self, domain, selector, serialized=False):
        """
        Get a single descriptor.
        /sel/ector/%hash
        /sel/ector/~version (where version is an integer. Negative values are
        evaluated from the most recent versions, counting backwards)
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
        return True
