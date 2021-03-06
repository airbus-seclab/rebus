import hashlib
import logging
import os
import uuid as m_uuid
from random import SystemRandom
from rebus.tools import format_check

log = logging.getLogger("rebus.descriptor")


class Descriptor(object):

    NAMESPACE_REBUS = m_uuid.uuid5(m_uuid.NAMESPACE_DNS, "rebus.airbus.com")

    def __init__(self, label, selector, value=None, domain="default",
                 agent=None, precursors=None, version=0, processing_time=-1,
                 uuid=None, bus=None):
        self.label = label
        """
        :param label: descriptor's label. Usually a file name, or
            human-understandable handle
        :param selector: descriptor's selector
        :param value: descriptor's value
        :param domain: descriptor's domain
        :param agent: descriptor's agent
        :param precursors: descriptor's precursors
        :param version: descriptor's version
        :param processing_time: descriptor's processing_time (automatically set
            in most cases)
        :param uuid: descriptor's uuid
        :param bus: descriptor's bus. None iif the value must be fetched from
            the bus

        May raise a ValueError if provided selector or descriptor are invalid
        """

        #: contains a list of parent descriptors' selectors. Typically contains
        #: 0 (ex. injected binaries), or (version+1) values
        self.precursors = precursors if precursors is not None else []

        self.agent = agent

        self.bus = bus

        if not format_check.is_valid_domain(domain):
            raise ValueError("invalid domain format (%s)" % domain)
        if not format_check.is_valid_selector(selector):
            raise ValueError("invalid selector format (%s)" % selector)
        p = selector.find("%")
        if p >= 0:
            h = selector[(p+1):]
            self.hash = h
        else:
            if self.agent and self.precursors:
                if type(value) is unicode:
                    strvalue = value.encode('utf-8')
                else:
                    strvalue = str(value)
                v = str(self.agent) + str(self.precursors) + selector + \
                    strvalue
            else:
                v = str(value)
            self.hash = hashlib.sha256(v).hexdigest()
            selector = os.path.join(selector, "%" + self.hash)
        self.selector = selector
        if self.bus is None:
            self.value = value
        self.domain = domain
        self.version = version
        #: if -1, will be set by agent when push() is called
        self.processing_time = processing_time
        if uuid is None:
            uuid = str(m_uuid.uuid5(self.NAMESPACE_REBUS, self.hash))
        #: A new uuid is generated for:
        #:
        #: * newly injected descriptors
        #: * descriptors that will have several versions
        #: * new versions of descriptors
        self.uuid = uuid

    @classmethod
    def new_with_randomhash(cls, label, selector, *args, **kwargs):
        """
        Helper to create a new Descriptor having a random hash.
        Useful in case the user wants to inject a previously-seen value, to
        force its re-processing by all agents.
        """
        random_hashstring = "%064x" % SystemRandom().getrandbits(256)
        selector = selector.split('%')[0] + '%' + random_hashstring
        return cls(label, selector, *args, **kwargs)

    def spawn_descriptor(self, selector, value, agent, processing_time=-1,
                         label=None):
        """
        Spawn a child descriptor.

        :param label: will use self's label if unset
        :param processing_time: agent.push() will set properly if equal to -1
        """
        if label is None:
            label = self.label
        desc = self.__class__(label, selector, value, self.domain,
                              agent=agent,
                              precursors=[self.selector],
                              processing_time=processing_time,
                              uuid=self.uuid)
        return desc

    def new_version(self, label, value, newprecursor, processing_time=-1):
        """
        Spawn a new version of a descriptor.
        Used for the output of analyzers that aggregate data from several
        descriptors that contain similar data types (ex. dismat contains
        dissimilarity values for descriptors whose datatype is
        /signature/sigtype/)

        :param label: will use self's label if unset
        :param processing_time: agent.push() will set properly if equal to -1
        :param newprecursor: selector of new precursor
        """
        desc = self.__class__(label, self.selector.split('%')[0],
                              value, self.domain,
                              agent=self.agent,
                              precursors=[newprecursor] + self.precursors,
                              version=self.version + 1,
                              processing_time=processing_time,
                              uuid=self.uuid)
        return desc

    def create_links(self, otherdesc, agentname, linktype, reason,
                     isSymmetric=False):
        """
        Creates and returns two /link/ descriptors
        Selector names for links: /link/agentname/linktype
        One link is created in both self's and otherdesc's UUID
        Value: dictionary containing origin selector, destination selector,
        reason for linking, link role to give a src/target logic
        [src/target/symmetric] and destination descriptor's label
        """
        if isSymmetric:
            link1role = 'symmetric'
            link2role = 'symmetric'
        else:
            link1role = 'src'
            link2role = 'target'

        link1 = Descriptor(
            label=self.label,
            selector='/link/%s/%s' % (agentname, linktype),
            value={'selector': self.selector,
                   'otherselector': otherdesc.selector,
                   'otherUUID': otherdesc.uuid,
                   'reason': reason,
                   'linkrole': link1role,
                   'linktype': linktype,
                   'otherlabel': otherdesc.label},
            domain=self.domain,
            agent=agentname,
            precursors=[self.selector, otherdesc.selector],
            uuid=self.uuid)

        link2 = Descriptor(
            label=otherdesc.label,
            selector='/link/%s/%s' % (agentname, linktype),
            value={'selector': otherdesc.selector,
                   'otherselector': self.selector,
                   'otherUUID': self.uuid,
                   'reason': reason,
                   'linkrole': link2role,
                   'linktype': linktype,
                   'otherlabel': self.label},
            domain=otherdesc.domain,
            agent=agentname,
            precursors=[self.selector, otherdesc.selector],
            uuid=otherdesc.uuid)
        return link1, link2

    def serialize(self, serializer):
        return serializer.dumps(
            {k: getattr(self, k) for k in dir(self)
             if k in ["label", "selector", "value", "domain", "agent",
                      "precursors", "version", "processing_time", "uuid"]})

    def serialize_meta(self, serializer):
        """
        Serialize descriptor, without its value.
        """
        # FIXME dumps may return non-ascii characters ("extended" ascii, "8-bit
        # ascii") which may result in invalid UTF-8, thus causing errors when
        # using dbus
        return serializer.dumps(
            {k: getattr(self, k) for k in dir(self)
             if k in ["label", "selector", "domain", "agent", "precursors",
                      "version", "processing_time", "uuid"]})

    def serialize_value(self, serializer):
        """
        Serialize descriptor value.
        """
        return serializer.dumps(self.value)

    @staticmethod
    def unserialize_value(serializer, s):
        return serializer.loads(s)

    @classmethod
    def unserialize(cls, serializer, s, bus=None):
        try:
            unserialized = serializer.loads(s)
        except ValueError:
            log.warning(
                "Invalid selector or domain encountered while "
                "unserializing a descriptor", exc_info=1)
            return None
        if unserialized:
            return cls(bus=bus, **unserialized)
        else:
            return None

    @property
    def value(self):
        if self.bus is None:
            return self._value
        else:
            self._value = self.bus.get_value(self.agent, self.domain,
                                             self.selector)
            self.bus = None
            return self._value

    @value.setter
    def value(self, value):
        self._value = value

    def __repr__(self):
        v = repr(self.value)
        if len(v) > 30:
            v = "[%i][%s...]" % (len(v), v[:22])
        return "%s:%s(%s)=%s" % (self.domain, self.selector, self.label.encode('utf-8'), v)

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __hash__(self):
        """
        self.hash is never changed
        """
        return hash(self.hash)
