from collections import Counter, defaultdict, namedtuple
from rebus.bus import Bus, DEFAULT_DOMAIN
from rebus.storage_backends.ramstorage import RAMStorage
import logging
import threading

log = logging.getLogger("rebus.localbus")
agent_desc = namedtuple("agent_desc", ("agent_id", "domain", "callback"))


@Bus.register
class LocalBus(Bus):
    _name_ = "localbus"

    def __init__(self, busaddr=None):
        Bus.__init__(self)
        self.callbacks = []
        self.locks = defaultdict(set)
        #: Next available agent id. Never decreases.
        self.agent_count = 0
        self.store = RAMStorage()  # TODO add support for DiskStorage ?
        # TODO save internal state at bus exit (only useful with DiskStorage)
        #: maps agentid (ex. inject-12) to agentdesc
        self.agent_descs = {}
        #: maps agentid to agent instance
        self.agents = {}
        self.threads = []
        #: maps agentids to their serialized configuration
        self.config_txts = {}

    def join(self, agent, agent_domain=DEFAULT_DOMAIN, callback=None):
        agid = "%s-%i" % (agent.name, self.agent_count)
        self.agent_count += 1
        if callback:
            # Always true when called from Agent - even if agent does not
            # overload process()
            self.callbacks.append((agid, callback))
        self.config_txts[agid] = agent.config_txt
        self.agent_descs[agid] = agent_desc(agid, agent_domain, callback)
        self.agents[agid] = agent
        return agid

    def lock(self, agent_id, lockid, desc_domain, selector):
        key = (lockid, desc_domain, selector)
        log.info("LOCK:%s %s => %r %s:%s", lockid, agent_id, key in
                 self.locks[desc_domain], desc_domain, selector)
        if key in self.locks[desc_domain]:
            return False
        self.locks[desc_domain].add(key)
        return True

    def push(self, agent_id, descriptor):
        desc_domain = descriptor.domain
        selector = descriptor.selector
        if self.store.add(descriptor):
            log.info("PUSH: %s => %s:%s", agent_id, desc_domain, selector)
            for agid, cb in self.callbacks:
                try:
                    log.debug("Calling %s callback", agid)
                    cb(agent_id, desc_domain, selector, False)
                except Exception as e:
                    log.error("ERROR agent [%s]: %s", agid, e, exc_info=1)
        else:
            log.info("PUSH: %s already seen => %s:%s", agent_id, desc_domain,
                     selector)

    def get(self, agent_id, desc_domain, selector):
        log.info("GET: %s %s:%s", agent_id, desc_domain, selector)
        return self.store.get_descriptor(desc_domain, selector,
                                         serialized=False)

    def get_value(self, agent_id, desc_domain, selector):
        log.info("GET: %s %s:%s", agent_id, desc_domain, selector)
        return self.store.get_value(desc_domain, selector)

    def list_uuids(self, agent_id, desc_domain):
        log.debug("LISTUUIDS: %s %s", agent_id, desc_domain)
        return self.store.list_uuids(desc_domain)

    def find(self, agent_id, desc_domain, selector_regex, limit):
        log.debug("FIND: %s %s:%s (%d)", agent_id, desc_domain, selector_regex,
                  limit)
        return self.store.find(desc_domain, selector_regex, limit)

    def find_by_uuid(self, agent_id, desc_domain, uuid):
        log.debug("FINDBYUUID: %s %s:%s", agent_id, desc_domain, uuid)
        return self.store.find_by_uuid(desc_domain, uuid, serialized=False)

    def find_by_value(self, agent_id, desc_domain, selector_prefix,
                      value_regex):
        log.debug("FINDBYVALUE: %s %s %s %s", agent_id, desc_domain,
                  selector_prefix, value_regex)
        return self.store.find_by_value(desc_domain, selector_prefix,
                                        value_regex, serialized=False)

    def mark_processed(self, desc_domain, selector, agent_id):
        agent_name = self.agents[agent_id].name
        config_txt = self.config_txts[agent_id]
        log.debug("MARK_PROCESSED: %s:%s %s %s", desc_domain, selector,
                  agent_id, config_txt)
        self.store.mark_processed(desc_domain, selector, agent_name,
                                  config_txt)

    def mark_processable(self, desc_domain, selector, agent_id):
        agent_name = self.agents[agent_id].name
        config_txt = self.config_txts[agent_id]
        log.debug("MARK_PROCESSABLE: %s:%s %s %s", desc_domain, selector,
                  agent_id, config_txt)
        self.store.mark_processable(desc_domain, selector, agent_name,
                                    config_txt)

    def list_agents(self, agent_id):
        return dict(Counter(i.rsplit('-', 1)[0]
                            for i in self.agent_descs.keys()))

    def processed_stats(self, agent_id, desc_domain):
        log.debug("PROCESSED_STATS: %s %s", agent_id, desc_domain)
        return self.store.processed_stats(desc_domain)

    def get_children(self, agent_id, desc_domain, selector, recurse=True):
        log.info("GET_CHILDREN: %s %s:%s", agent_id, desc_domain, selector)
        return list(self.store.get_children(desc_domain, selector,
                                            recurse, serialized=False))

    def store_internal_state(self, agent_id, state):
        log.debug("STORE_INTSTATE: %s", agent_id)
        if self.store.STORES_INTSTATE:
            agent_name = self.agents[agent_id].name
            self.store.store_state(agent_name, str(state))

    def load_internal_state(self, agent_id):
        log.debug("LOAD_INTSTATE: %s", agent_id)
        if self.store.STORES_INTSTATE:
            agent_name = self.agents[agent_id].name
            return self.store.load_state(agent_name)
        return ""

    def request_processing(self, agent_id, desc_domain, selector,
                           targets):
        for agid, cb in self.callbacks:
            if self.agents[agid].name in targets:
                try:
                    log.debug("Calling %s callback for user-requested "
                              "processing", agid)
                    cb(agent_id, desc_domain, selector, True)
                except Exception as e:
                    log.error("ERROR agent [%s]: %s", agid, e, exc_info=1)

    def run_agents(self):
        for agent in self.agents.values():
            t = threading.Thread(target=agent.run)
            t.daemon = True
            t.start()
            self.threads.append(t)
        for t in self.threads:
            t.join()
