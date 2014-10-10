import cPickle
import hashlib
import logging
import os
import uuid as m_uuid

log = logging.getLogger("rebus.descriptor")


class Descriptor(object):

    NAMESPACE_REBUS = m_uuid.uuid5(m_uuid.NAMESPACE_DNS, "rebus.airbus.com")

    def __init__(self, label, selector, value, domain="default",
                 agent=None, precursors=None, version=0, processing_time=-1,
                 uuid=None):
        self.label = label

        #: contains a list of parent descriptors' selectors. Typically contains
        #: 0 (ex. injected binaries), or (version+1) values
        self.precursors = precursors if precursors is not None else []

        self.agent = agent

        p = selector.rfind("%")
        if p >= 0:
            self.hash = selector[(p + 1):]
        else:
            if self.agent and self.precursors:
                v = str(self.agent) + str(self.precursors) + selector
            else:
                v = value if type(value) is str else cPickle.dumps(value)
            self.hash = hashlib.sha256(v).hexdigest()
            selector = os.path.join(selector, "%" + self.hash)
        self.selector = selector
        self.value = value
        self.domain = domain
        self.version = version
        self.processing_time = processing_time
        if uuid is None:
            uuid = str(m_uuid.uuid5(self.NAMESPACE_REBUS, self.hash))
        #: A new uuid is generated for:
        #:
        #: * newly injected descriptors
        #: * descriptors that will have several versions
        #: * new versions of descriptors
        self.uuid = uuid

    def spawn_descriptor(self, selector, value, agent, processing_time=-1,
                         label=None):
        # Allow changing labels, e.g. when spawned descriptor contains same
        # data type as its precursor (ex. binary -> unpacked binary)
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
        """
        desc = self.__class__(label, self.selector.split('%')[0],
                              value, self.domain,
                              agent=self.agent,
                              precursors=[newprecursor] + self.precursors,
                              version=self.version + 1,
                              processing_time=processing_time)
        return desc

    def create_links(self, otherdesc, agentname, short_reason, reason):
        """
        Creates and returns two /link/ descriptors
        Selector names for links: /link/agentname/short_reason/other_UUID
        One link is created in both self's and otherdesc's UUID
        Value: dictionary containing origin selector, destination selector,
        reason for linking and destination descriptor's label
        """
        link1 = Descriptor(
            label=self.label,
            selector='/link/%s/%s/%s' % (agentname, short_reason,
                                         otherdesc.uuid),
            value={'selector': self.selector,
                   'otherselector': otherdesc.selector,
                   'reason': reason,
                   'otherlabel': otherdesc.label},
            domain="default",
            agent=agentname,
            precursors=[self.selector, otherdesc.selector],
            uuid=self.uuid)

        link2 = Descriptor(
            label=otherdesc.label,
            selector='/link/%s/%s/%s' % (agentname, short_reason, self.uuid),
            value={'selector': otherdesc.selector,
                   'otherselector': self.selector,
                   'reason': reason,
                   'otherlabel': self.label},
            domain="default",
            agent=agentname,
            precursors=[self.selector, otherdesc.selector],
            uuid=otherdesc.uuid)
        return link1, link2

    def serialize(self):
        return cPickle.dumps(
            {k: v for k, v in self.__dict__.iteritems()
             if k in ["label", "selector", "value", "domain", "agent",
                      "precursors", "version", "processing_time", "uuid"]})

    @classmethod
    def unserialize(cls, s):
        unserialized = cPickle.loads(s)
        if unserialized:
            return cls(**unserialized)
        else:
            return None

    def __repr__(self):
        v = repr(self.value)
        if len(v) > 30:
            v = "[%i][%s...]" % (len(v), v[:22])
        return "%s:%s(%s)=%s" % (self.domain, self.selector, self.label, v)
