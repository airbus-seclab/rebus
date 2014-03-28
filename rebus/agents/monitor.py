from rebus.agent import Agent


@Agent.register
class Monitor(Agent):
    _name_ = "monitor"
    _desc_ = "Dump all descriptors exchanged on the bus"

    def process(self, desc, sender_id):
        print "=" * 60
        print "From=%s" % sender_id
        print "Domain=%s" % desc.domain
        print "Label=%s" % desc.label
        print "Selector=%s" % desc.selector
        v = repr(desc.value)
        print "Len=%i" % len(v)
        print "-" * 60
        print v[:1500]
        print "=" * 60
