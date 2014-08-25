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
        self.dstore = defaultdict(dict)
        self.serialized_store = defaultdict(OrderedDict)

        # self.edges['domain']['selectorA'] is a set of selectors of
        # descriptors that were spawned from selectorA.
        self.edges = defaultdict(lambda: defaultdict(set))

    def find(self, domain, selector_regex, limit=1):
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
        for k,v in reversed(store.items()):
            if regex.match(k):
                res.append(k)
                if len(res) >= limit:
                    return res
        return res

    def get_descriptor(self, domain, selector, serialized=False):
        """
        Get a single descriptor.
        /sel/ector/%hash
        /sel/ector/!version (where version is an integer or "latest")
        """
        # if version is specified, but no hash
        if '%' not in selector and '!' in selector:
            selprefix, version = selector.split('!')
            latestv = -1
            latestvhash = ""
            for k, v in reversed(self.dstore[domain].items()):
                if k.startswith(selprefix):
                    if str(v.version) == version:
                        selector = selprefix + '%' + v.hash
                        break
                    if v.version > latestv:
                        latestv = v.version
                        latestvhash = v.hash
            if version == 'latest' and latestvhash:
                selector = selprefix + '%' + latestvhash
        # TODO : handle bad selector !
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
        for precursor in descriptor.precursors:
            self.edges[domain][precursor].add(selector)
        return True
