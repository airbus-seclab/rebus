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
        Return list of selectors according to search constraints :

        * domain
        * selector regular expression
        * limit (max number of entries to return)

        :param domain: string, domain on which operations are performed
        :param selector_regex: string, regex
        :param limit: int, max number of selectors to return. Unlimited if 0.

        Only selectors of *limit* most recently added descriptors will be
        returned, from most recent to oldest.
        """
        raise NotImplementedError

    def find_by_selector(self, domain, selector_prefix):
        """
        Return a list of descriptors whose selector starts with
        selector_prefix.

        Unspecified list order - may vary depending on the backend.
        """
        raise NotImplementedError

    def find_by_uuid(self, domain, uuid):
        """
        Return a list of descriptors whose uuid match given parameter.

        Unspecified list order - may vary depending on the backend.
        """
        raise NotImplementedError

    def find_by_value(self, domain, selector_prefix, value_regex):
        """
        Return a list of matching descriptors:

        * desc.domain == desc_domain
        * desc.selector.startswith(selector_prefix)
        * re.match(value_regex, desc.value)

        Unspecified list order - may vary depending on the backend.
        """
        raise NotImplementedError

    def list_uuids(self, domain):
        """
        :param domain: domain from which UUID should be enumerated

        Return a dictionary mapping known UUIDs to corresponding labels.

        Unspecified list order - may vary depending on the backend.
        """
        raise NotImplementedError

    def get_descriptor(self, domain, selector):
        """
        Get a single descriptor.
        /sel/ector/%hash
        /sel/ector/~version (where version is an integer. Negative values are
        evaluated from the most recent versions, counting backwards)

        :param domain: string, domain on which operations are performed
        :param selector: string
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

    def get_children(self, domain, selector, recurse=True):
        """
        Return a set of children descriptors from given selector.

        :param domain: string, domain on which operations are performed
        :param selector: string
        :param recurse: boolean, recursively fetch children if True
        """
        raise NotImplementedError

    def add(self, descriptor):
        """
        Add new descriptor to storage. Return False if descriptor was already
        present, else False.

        :param descriptor: descriptor to be stored
        """
        raise NotImplementedError

    def mark_processed(self, domain, selector, agent_name, config_txt):
        """
        Mark given selector as having been processed by given agent whose
        configuration is serialized in config_txt.

        Returns a boolean indicating whether this selector had not already been
        marked as processed or processable by this (agent, config_txt)

        :param domain: string, domain on which operations are performed
        :param selector: string
        :param agent_name: string, agent name
        :param config_txt: string, serialized configuration of agent
            describing output altering options
        """
        raise NotImplementedError

    def mark_processable(self, domain, selector, agent_name, config_txt):
        """
        Mark given selector as processable by given agent running in
        interactive mode whose configuration is serialized in config_txt.

        Returns a boolean indicating whether this selector had not already been
        marked as processed by this (agent, config_txt).

        :param domain: string, domain on which operations are performed
        :param selector: string
        :param agent_name: string, agent name
        :param config_txt: string, serialized configuration of agent
            describing output altering options
        """
        raise NotImplementedError

    def get_processed(self, domain, selector):
        """
        Return the set of (agents, config_txt) that have processed this
        selector.

        :param domain: string, domain on which operations are performed
        :param selector: string
        """
        raise NotImplementedError

    def get_processable(self, domain, selector):
        """
        Return the set of (agents, config_txt) running in interactive mode
        that could process this selector.

        :param domain: string, domain this selector belongs to
        :param selector: string
        """
        raise NotImplementedError

    def processed_stats(self, domain):
        """
        Returns a list of couples, (agent names, number of processed selectors)
        and the total amount of selectors in this domain.
        """
        raise NotImplementedError

    def store_agent_state(self, agent_name, state):
        """
        Store serialized agent state.

        :param agent_name: string, agent name
        :param state: string, serialized internal state of agent
        """
        raise NotImplementedError

    def load_agent_state(self, agent_name):
        """
        Return serialized agent state.

        :param agent_name: string, agent name
        :param state: string, serialized internal state of agent
        """
        raise NotImplementedError

    def store_state(self):
        """
        May be used to store storage state.
        """
        pass

    def list_unprocessed_by_agent(self, agent_name, config_txt):
        """
        Return a list of (domain, uuid, selector) that have not been processed
        by this agent, identified by its name.

        :param agent_name: string, agent name
        :param config_txt: string, serialized configuration of agent
            describing output altering options
        """
        return []

    @staticmethod
    def add_arguments(subparser):
        """
        Allow storage backend to receive optional arguments
        """
        pass
