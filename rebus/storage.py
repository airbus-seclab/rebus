#!/usr/bin/env python2


from collections import defaultdict


class DescriptorStorage(object):
    """
    Storage and retrieval of descriptor objects.
    """
    def __init__(self):
        # self.dstore['domain']['/selector/%hash'] is a serialized descriptor
        self.dstore = defaultdict(dict)
        self.serialized_store = defaultdict(dict)

        # self.edges['domain']['selectorA'] is a set of selectors of descriptors that were
        # spawned from selectorA.
        self.edges = defaultdict(lambda: defaultdict(set))


    def find(self, constraints={}, limit=1):
        """
        Specify search constraints :
        * domain
        * hash
        * selector
        """
        # TODO
        pass


    def get_descriptor(self, domain, selector, serialized=False):
        """
        Get a single descriptor.
        """
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
