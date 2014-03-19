
import threading
from collections import defaultdict,namedtuple
from rebus.bus import Bus, DEFAULT_DOMAIN
import logging

log = logging.getLogger("rebus.localbus")


agent_desc = namedtuple("agent_desc", ("agent_id", "domain", "callback"))

@Bus.register
class LocalBus(Bus):
    _name_ = "localbus"
    def __init__(self, busaddr=None):
        Bus.__init__(self)
        self.callbacks = defaultdict(list)
        self.locks = defaultdict(set)
        self.selectors = defaultdict(dict)
        self.agent_count = 0
        self.agents = {}
        self.threads = []
    def join(self, name, domain=DEFAULT_DOMAIN, callback=None):
        agid = "%s-%i" % (name,self.agent_count)
        self.agent_count += 1
        if callback:
            self.callbacks[domain].append((agid,callback))
        self.agents[agid] = agent_desc(agid, domain, callback)
        return agid

    def lock(self, agent, lockid, selector):
        domain = self.agents[agent.id].domain
        key = (lockid,selector)
        if key in self.locks[domain]:
            return False
        self.locks[domain].add(key)
        return True
    def push(self, agent, selector, descriptor):
        domain = self.agents[agent.id].domain
        if selector in self.selectors[domain]:
            pass
        else:
            log.info("Adding for %s: %s:%s" % (agent.id, domain, selector))
            self.selectors[domain][selector] = descriptor
            for agid,cb in self.callbacks[domain]:
                if agid != agent.id:
                    try:
                        log.debug("Calling %s callback" % agid)
                        cb(agent.id, domain, selector)
                    except Exception,e:
                        log.error("ERROR agent [%s]: %s" % (agid, e))
    def get(self, agent, selector):
        domain = self.agents[agent.id].domain
        return self.selectors[domain].get(selector)
    def get_selectors(self, agent, selector_filter="/"):
        domain = self.agents[agent.id].domain
        return [ s
                 for s in self.selectors[domain].itervalues()
                 if s.selector.startswith(selector) ]

    def run_agent(self, agent, args):
        t = threading.Thread(target=agent.run, args=args)
        t.daemon = True
        t.start()
        self.threads.append(t)
    def agentloop(self, agent):
        pass
    def busloop(self):
        for t in self.threads:
            t.join()

