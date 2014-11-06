#!/usr/bin/env python2
from rebus.tools.registry import Registry


class StorageRegistry(Registry):
    pass


class Storage(object):
    _name_ = "Storage"
    _desc_ = "N/A"

    #: Indicates whether the storage backend stores agent's internal state
    STORES_INTSTATE = False

    @staticmethod
    def register(f):
        return StorageRegistry.register_ref(f, key="_name_")

    def __init__(self, **kwargs):
        pass

    def find(self, domain, selector_regex, limit):
        """
        Return array of selectors according to search constraints :

        * domain
        * selector regular expression
        * limit (max number of entries to return)

        :param domain: string, domain on which operations are performed
        :param selector_regex: string, regex
        :param limit: int, max number of selectors to return. Unlimited if 0.
        """
        raise NotImplementedError

    def find_by_uuid(self, domain, uuid, serialized=False):
        """
        Return a list of descriptors whose uuid match given parameter
        """
        raise NotImplementedError

    def list_uuids(self, domain):
        """
        :param domain: domain from which UUID should be enumerated

        Return a dictionary mapping known UUIDs to corresponding labels.
        """
        raise NotImplementedError

    def get_descriptor(self, domain, selector, serialized=False):
        """
        Get a single descriptor.
        /sel/ector/%hash
        /sel/ector/~version (where version is an integer. Negative values are
        evaluated from the most recent versions, counting backwards)

        :param domain: string, domain on which operations are performed
        :param selector: string
        :param serialized: boolean, return serialized descriptors if True
        """
        raise NotImplementedError

    def get_value(self, domain, selector):
        """
        Get a selector's value.
        /sel/ector/%hash

        :param domain: string, domain on which operations are performed
        :param selector: string
        """
        raise NotImplementedError

    def get_children(self, domain, selector, serialized=False, recurse=True):
        """
        Return a set of children descriptors from given selector.

        :param domain: string, domain on which operations are performed
        :param selector: string
        :param serialized: boolean, return serialized descriptors if True
        :param recurse: boolean, recursively fetch children if True
        """
        raise NotImplementedError

    def add(self, descriptor, serialized_descriptor=None):
        """
        Add new descriptor to storage

        :param descriptor: descriptor to be stored
        :param serialized_descriptor: string, optionally contains a serialized
            version of the descriptor
        """
        raise NotImplementedError

    def mark_processed(self, domain, selector, agent_id, config_txt):
        """
        Mark given selector as having been processed by given agent whose
        configuration is serialized in config_txt.

        :param domain: string, domain on which operations are performed
        :param selector: string
        :param agent_id: string, agent name
        :param config_txt: string, JSON-serialized configuration of agent
        """
        raise NotImplementedError

    def get_processed(self, domain, selector):
        """
        Return the list of (agents, config_txt) that have processed this
        selector.

        :param domain: string, domain on which operations are performed
        :param selector: string
        """
        raise NotImplementedError

    def store_state(self, agent_id, state):
        """
        Store serialized agent state.

        :param agent_id: string, agent name
        :param state: string, serialized internal state of agent
        """
        raise NotImplementedError

    def load_state(self, agent_id):
        """
        Return serialized agent state.

        :param agent_id: string, agent name
        :param state: string, serialized internal state of agent
        """
        raise NotImplementedError

    @staticmethod
    def add_arguments(subparser):
        """
        Allow storage backend to receive optional arguments
        """
        pass
