from rebus.agent import Agent


@Agent.register
class Monitor(Agent):
    _name_ = "monitor"
    _desc_ = "Dump all descriptors exchanged on the bus"
    _operationmodes_ = ('automatic', )

    def process(self, descriptor, sender_id):
        print "=" * 60
        print "From=%s" % sender_id
        print "Domain=%s" % descriptor.domain
        print "Label=%s" % descriptor.label
        print "Selector=%s" % descriptor.selector
        v = repr(descriptor.value)
        print "Len=%i" % len(v)
        print "-" * 60
        print v[:1500]
        print "=" * 60
