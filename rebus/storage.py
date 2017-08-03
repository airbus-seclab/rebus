#!/usr/bin/env python2
from rebus.tools.registry import Registry
import threading
import re
import sqlite3


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

    def __init__(self, options=None):
        """
        :param options: argparse.Namespace containing storage option
        """
        pass

    def find(self, domain, selector_regex, limit=0, offset=0):
        """
        Return list of selectors according to search constraints:

        :param domain: string, domain in which the search is performed
        :param selector_regex: string, regex
        :param limit: int, max number of matching selectors to return.
            Unlimited if 0.
        :param offset: int, number of selectors to skip.

        Only selectors of *limit* most recently added descriptors will be
        returned, sorted from most recent to oldest.
        """
        raise NotImplementedError

    def find_by_selector(self, domain, selector_prefix, limit=0, offset=0):
        """
        Return a list of descriptors whose selector starts with
        selector_prefix.

        :param domain: string, domain in which the search is performed
        :param selector_prefix: string
        :param limit: int, max number of matching descriptors to return.
            Unlimited if 0.
        :param offset: int, number of matching descriptors to skip.

        The returned list is sorted, from oldest to newest.
        """
        raise NotImplementedError

    def find_by_uuid(self, domain, uuid):
        """
        Return a list of descriptors whose uuid match given parameter.

        :param domain: string, domain in which the search is performed
        :param uuid: string, uuid in which the search is performed
        :param offset: int, number of selectors to skip.
        :param limit: int, max number of selectors to return. Unlimited if 0.

        Unspecified list order - may vary depending on the backend.
        """
        raise NotImplementedError

    def find_by_value(self, domain, selector_prefix, value_regex):
        """
        Return a list of matching descriptors:

        * desc.domain == desc_domain
        * desc.selector.startswith(selector_prefix)
        * re.match(value_regex, desc.value)

        :param domain: string, domain in which the search is performed
        :param selector_prefix: string
        :param value_regex: string, regex
        :param offset: int, number of selectors to skip.
        :param limit: int, max number of selectors to return. Unlimited if 0.

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

        Returns None if descriptor could not be found.

        :param domain: string, domain on which operations are performed
        :param selector: string
        """
        raise NotImplementedError

    def get_value(self, domain, selector):
        """
        Get a selector's value.
        /sel/ector/%hash

        Returns None if descriptor could not be found.

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
        present, else True.

        :param descriptor: descriptor to be stored
        """
        raise NotImplementedError

    def mark_processed(self, domain, selector, agent_name, config_txt):
        """
        Mark given selector as having been processed by given agent whose
        configuration is serialized in config_txt.

        Returns True if this selector had not already been marked processed or
        processable by this (agent, config_txt)

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

        Returns True if this selector had not already been marked processed or
        processable by this (agent, config_txt).

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


class MetadataDB(object):
    def __init__(self, db_path):
        self._dblock = threading.RLock()
        self._db = sqlite3.connect(db_path)
        self._cursor = self._db.cursor()
        self._cursor.execute(
            'CREATE TABLE IF NOT EXISTS processed(domain TEXT, selector TEXT, '
            'agent_name TEXT, config_txt TEXT)')
        self._cursor.execute(
            'CREATE UNIQUE INDEX IF NOT EXISTS no_processed_dups ON '
            'processed(domain, selector, agent_name, config_txt)')
        self._cursor.execute(
            'CREATE TABLE IF NOT EXISTS selectors(domain TEXT, selector TEXT)')
        self._cursor.execute(
            'CREATE UNIQUE INDEX IF NOT EXISTS no_selector_dups ON '
            'selectors(domain, selector)')
        self._db.commit()

        def regex_function(pattern, string):
            return re.match(pattern, string) is None

        self._db.create_function('REGEXP', 2, regex_function)

    def add_selector(self, domain, selector):
        with self._dblock:
            self._cursor.execute(
                'INSERT OR IGNORE INTO selectors(domain, selector) '
                'VALUES (?, ?)',
                (domain, selector))
            self._db.commit()

    def add_processed(self, domain, selector, agent_name, config_txt):
        """
        Returns True if this (domain, selector) had not already been marked as
        processed by this (agent_name, config_txt)
        """
        with self._dblock:
            try:
                self._cursor.execute(
                    'INSERT OR ABORT INTO processed(domain, selector, '
                    'agent_name, config_txt) VALUES (?, ?, ?, ?)',
                    (domain, selector, agent_name, config_txt))
                self._db.commit()
                return True
            except sqlite3.IntegrityError:
                return False

    def is_processed(self, domain, selector, agent_name, config_txt):
        with self._dblock:
            res = self._cursor.execute(
                'SELECT COUNT(1) FROM PROCESSED WHERE domain=? AND selector=? '
                'AND agent_name=? AND config_txt=?',
                (domain, selector, agent_name, config_txt)
            ).fetchone()[0]
            return res == 1

    def list_processed(self, domain, selector):
        with self._dblock:
            res = self._cursor.execute(
                'SELECT agent_name, config_txt FROM processed WHERE '
                'domain=? AND selector=?', (domain, selector)).fetchall()
            return {(str(agent_name), str(config_txt)) for
                    (agent_name, config_txt) in res}

    def processed_stats(self, domain):
        # FIXME take domain into account
        with self._dblock:
            by_agent = self._cursor.execute(
                'SELECT agent_name, COUNT(DISTINCT selector) FROM processed '
                'WHERE domain=? GROUP BY agent_name',
                (domain,)).fetchall()
            total = self._cursor.execute(
                'SELECT COUNT(DISTINCT selector) FROM processed '
                'WHERE domain=?',
                (domain,)).fetchone()[0]
        return ([(str(agent_name), count) for agent_name, count in by_agent],
                total)

    def list_unprocessed_by_agent(self, agent_name, config_txt):
        with self._dblock:
            unprocessed = self._cursor.execute(
                'SELECT domain, selector FROM selectors '
                'GROUP BY domain, selector EXCEPT '
                'SELECT domain, selector FROM processed '
                'WHERE agent_name=? AND config_txt=? '
                'GROUP BY domain, selector',
                (agent_name, config_txt)).fetchall()
        return [(str(domain), str(selector)) for domain, selector in
                unprocessed]

    def find(self, domain, selector_regex, limit, offset):
        if limit == 0:
            # no limit
            limit = -1
        with self._dblock:
            res = self._cursor.execute(
                'SELECT selector FROM processed '
                'WHERE selector REGEXP ? AND DOMAIN=? '
                'ORDER BY _rowid_ DESC '
                'LIMIT ? OFFSET ?',
                (domain, selector_regex, limit, offset)
            ).fetchall()
        return [str(selector) for selector in res]

    def find_by_selector(self, domain, selector_prefix, limit, offset):
        with self._dblock:
            selector_prefix.replace('%', '_')  # matches 1 character
            selector_prefix += '%'
            res = self._cursor.execute(
                'SELECT selector FROM processed '
                'WHERE selector LIKE ? AND DOMAIN=? '
                'ORDER BY _rowid_ DESC '
                'LIMIT ? OFFSET ?',
                (domain, selector_prefix, limit, offset)
            ).fetchall()
        return [str(selector) for selector in res]
