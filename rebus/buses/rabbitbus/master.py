#! /usr/bin/env python

import sys
import time
import signal
from collections import Counter, defaultdict
from rebus.descriptor import Descriptor
import logging
from rebus.tools.config import get_output_altering_options
import pika

try:
    import cPickle as pickle
except:
    import pickle
import uuid

log = logging.getLogger("rebus.bus")

class RabbitBusMaster():
    def __init__(self, store, server_addr):
        self.store = store
        #: maps agentid (ex. inject-:1.234) to object path (ex:
        #: /agent/inject)
        self.clients = {}
        self.exiting = False
        #: processed[domain] is a set of (lockid, selector) whose processing
        #: has started (might even be finished). Allows several agents that
        #: perform the same stateless computation to run in parallel
        self.processed = defaultdict(set)
        signal.signal(signal.SIGTERM, self.sigterm_handler)
        #: maps agentids to their names
        self.agentnames = {}
        #: maps agentids to their serialized configuration - output altering
        #: options only
        self.agents_output_altering_options = {}
        #: maps agentids to their serialized configuration
        self.agents_full_config_txts = {}
        #: monotonically increasing user request counter
        self.userrequestid = 0
        #: number of descriptors
        self.descriptor_count = 0
        #: count descriptors marked as processed/processable by each uniquely
        #: configured agent
        self.descriptor_handled_count = {}
        #: dict keyed by "uniquely configured agents", i.e.
        #: (agent_name, config_txt) listing agent_ids
        self.uniq_conf_clients = defaultdict(list)

        # Connects to the rabbitmq server
        if server_addr is None:
            server_addr = "amqp://localhost/%2F?connection_attempts=200&heartbeat_interval=1"
        else:
            server_addr = server_addr + "/%2F?connection_attempts=200&heartbeat_interval=1"
        params = pika.URLParameters(server_addr)
        try:
            self.connection = pika.BlockingConnection(params)
        except pika.exceptions.ConnectionClosed:
            log.warning("Cannot connect to rabbitmq at: " + str(busaddr))
            # TODO: quit here (failed to connect)

        self.channel = self.connection.channel()

        # Create the registration queue and push 10000 UID in it
        # TODO: publish a new ID when a new client register
        self.channel.queue_declare(queue="registration_queue", auto_delete=True)
        for id in range(10000):
            self.channel.basic_publish(exchange = "", routing_key = "registration_queue",
                                       body=str(1), properties=pika.BasicProperties(delivery_mode = 2,))
        # Create the exchange for signals publish(master)/subscribe(slave)
        self.signal_exchange = self.channel.exchange_declare(exchange='rebus_signals', type='fanout')
        
        # Create the rpc queue
        self.channel.queue_declare(queue='rebus_master_rpc', auto_delete=True)
        self.channel.basic_consume(self.rpc_callback, queue='rebus_master_rpc')

    def send_signal(self, signal_name, args):
        # Send a signal on the exchange
        body = {'signal_name' : signal_name, 'args' : args}
        body = pickle.dumps(body, protocol=2)
        self.channel.basic_publish(exchange='rebus_signals', routing_key='', body=body)

    #TODO Check is the key is valid
    def call_rpc_func(self, name, args):
        f = { 'register' : self.register,
              'unregister' : self.unregister,
              'lock' : self.lock,
              'push' : self.push,
              'get' : self.get,
              'get_value' : self.get_value,
              'list_uuids' : self.list_uuids,
              'find' : self.find,
              'find_by_uuid' : self.find_by_uuid,
              'find_by_value' : self.find_by_value,
              'mark_processed' : self.mark_processed,
              'mark_processable' : self.mark_processable,
              'get_processable' : self.get_processable,
              'list_agents' : self.list_agents,
              'processed_stats' : self.processed_stats,
              'get_children' : self.get_children,
              'store_internal_state' : self.store_internal_state,
              'load_internal_state' : self.load_internal_state,
              'request_processing' : self.request_processing,
            }
        return f[name](**args)
        
    def rpc_callback(self, ch, method, properties, body):
        # Parse the rpc request
        body = pickle.loads(body)
        func_name = body['func_name']
        args = body['args']

        # Call the function
        ret = self.call_rpc_func(func_name, args)
        ret = pickle.dumps(ret, protocol=2)

        # Push the result of the function on the return queue
        retpublish = ch.basic_publish(exchange='',
                                      routing_key=properties.reply_to,
                                      body=ret,
                                      properties=pika.BasicProperties(correlation_id = \
                                                                      properties.correlation_id))
        ch.basic_ack(delivery_tag = method.delivery_tag)

        
    def update_check_idle(self, agent_name, output_altering_options):
        """
        Increases the count of handled descriptors and checks
        if all descriptors have been handled (processed/marked
        as processable).
        In that case, send the "on_idle" message.
        """
        name_config = (agent_name, output_altering_options)
        self.descriptor_handled_count[name_config] += 1
        # Check if we have reached idle state
        nbdistinctagents = len(self.descriptor_handled_count)
        nbhandlings = sum(self.descriptor_handled_count.values())
        if self.descriptor_count*nbdistinctagents == nbhandlings:
            log.debug("IDLE: %d agents having distinct (name, config) %d "
                      "descriptors %d handled", nbdistinctagents,
                      self.descriptor_count, nbhandlings)
            self.on_idle()
        return

    def register(self, agent_id, agent_domain, pth, config_txt):
        #: indicates whether another instance of the same agent is already
        #: running with the same configuration
        agent_name = agent_id.split('-', 1)[0]
        self.agentnames[agent_id] = agent_name
        output_altering_options = get_output_altering_options(str(config_txt))

        name_config = (agent_name, output_altering_options)
        already_running = len(self.uniq_conf_clients[name_config]) > 1
        self.uniq_conf_clients[name_config].append(agent_id)

        self.clients[agent_id] = pth
        self.agents_output_altering_options[agent_id] = output_altering_options
        self.agents_full_config_txts[agent_id] = str(config_txt)
        log.info("New client %s (%s) in domain %s with config %s", pth,
                 agent_id, agent_domain, config_txt)
        # Send not-yet processed descriptors to the agent...
        if not already_running:
            # ...unless another instance of the same agent has already been
            # started, and should be processing those descriptors
            unprocessed = \
                self.store.list_unprocessed_by_agent(agent_name,
                                                     output_altering_options)
            self.descriptor_handled_count[name_config] = \
                self.descriptor_count - len(unprocessed)
            for dom, uuid, sel in unprocessed:
                self.targeted_descriptor("storage", dom, uuid, sel,
                                         [agent_name], False)

    def unregister(self, agent_id):
        log.info("Agent %s has unregistered", agent_id)
        agent_name = self.agentnames[agent_id]
        options = self.agents_output_altering_options[agent_id]
        name_config = (agent_name, options)
        self.uniq_conf_clients[name_config].remove(agent_id)
        if len(self.uniq_conf_clients[name_config]) == 0:
            del self.descriptor_handled_count[name_config]
        del self.clients[agent_id]
        if self.exiting:
            if len(self.clients) == 0:
                log.info("Exiting - no agents are running")
                self.channel.stop_consuming()
            else:
                log.info("Expecting %u more agents to exit (ex. %s)",
                         len(self.clients), self.clients.keys()[0])

    def lock(self, agent_id, lockid, desc_domain, selector):
        print "In lock function"
        objpath = self.clients[agent_id]
        processed = self.processed[desc_domain]
        key = (lockid, selector)
        log.debug("LOCK:%s %s(%s) => %r %s:%s ", lockid, objpath, agent_id,
                  key in processed, desc_domain, selector)
        print "if key in processed then false else true"
        if key in processed:
            print "lock return False"
            return False
        print "processed.add(key)"
        processed.add(key)
        print "lock return True"
        return True

    def push(self, agent_id, descriptor):
        unserialized_descriptor = Descriptor.unserialize(str(descriptor))
        desc_domain = str(unserialized_descriptor.domain)
        uuid = str(unserialized_descriptor.uuid)
        selector = str(unserialized_descriptor.selector)
        if self.store.add(unserialized_descriptor,
                          serialized_descriptor=str(descriptor)):
            self.descriptor_count += 1
            log.debug("PUSH: %s => %s:%s", agent_id, desc_domain, selector)
            self.new_descriptor(agent_id, desc_domain, uuid, selector)
            return True
        else:
            log.debug("PUSH: %s already seen => %s:%s", agent_id, desc_domain,
                      selector)
            return False

    def get(self, agent_id, desc_domain, selector):
        log.debug("GET: %s %s:%s", agent_id, desc_domain, selector)
        return self.store.get_descriptor(str(desc_domain), str(selector),
                                         serialized=True)

    def get_value(self, agent_id, desc_domain, selector):
        log.debug("GETVALUE: %s %s:%s", agent_id, desc_domain, selector)
        return self.store.get_value(str(desc_domain), str(selector), True)

    def list_uuids(self, agent_id, desc_domain):
        log.debug("LISTUUIDS: %s %s", agent_id, desc_domain)
        return self.store.list_uuids(str(desc_domain))

    def find(self, agent_id, desc_domain, selector_regex, limit):
        log.debug("FIND: %s %s:%s (%d)", agent_id, desc_domain, selector_regex,
                  limit)
        return self.store.find(str(desc_domain), str(selector_regex),
                               str(limit))

    def find_by_uuid(self, agent_id, desc_domain, uuid):
        log.debug("FINDBYUUID: %s %s:%s", agent_id, desc_domain, uuid)
        return self.store.find_by_uuid(str(desc_domain), str(uuid),
                                       serialized=True)

    def find_by_value(self, agent_id, desc_domain, selector_prefix,
                      value_regex):
        log.debug("FINDBYVALUE: %s %s %s %s", agent_id, desc_domain,
                  selector_prefix, value_regex)
        return self.store.find_by_value(str(desc_domain), str(selector_prefix),
                                        str(value_regex), serialized=True)

    def mark_processed(self, agent_id, desc_domain, selector):
        agent_name = self.agentnames[agent_id]
        options = self.agents_output_altering_options[agent_id]
        log.debug("MARK_PROCESSED: %s:%s %s %s", desc_domain, selector,
                  agent_id, options)
        isnew = self.store.mark_processed(str(desc_domain), str(selector),
                                          agent_name, str(options))
        if isnew:
            self.update_check_idle(agent_name, options)

    def mark_processable(self, agent_id, desc_domain, selector):
        agent_name = self.agentnames[agent_id]
        options = self.agents_output_altering_options[agent_id]
        log.debug("MARK_PROCESSABLE: %s:%s %s %s", desc_domain, selector,
                  agent_id, options)
        isnew = self.store.mark_processable(str(desc_domain), str(selector),
                                            agent_name, str(options))
        if isnew:
            self.update_check_idle(agent_name, options)

    def get_processable(self, agent_id, desc_domain, selector):
        log.debug("GET_PROCESSABLE: %s:%s %s", desc_domain, selector, agent_id)
        return self.store.get_processable(str(desc_domain), str(selector))

    def list_agents(self, agent_id):
        log.debug("LIST_AGENTS: %s", agent_id)
        #: maps agent name to number of instances of this agent
        counts = dict(Counter(objpath.rsplit('/', 1)[1] for objpath in
                              self.clients.values()))
        return counts

    def processed_stats(self, agent_id, desc_domain):
        log.debug("PROCESSED_STATS: %s %s", agent_id, desc_domain)
        return self.store.processed_stats(str(desc_domain))

    def get_children(self, agent_id, desc_domain, selector, recurse):
        log.debug("GET_CHILDREN: %s %s:%s", agent_id, desc_domain, selector)
        return list(self.store.get_children(str(desc_domain), str(selector),
                                            serialized=True,
                                            recurse=bool(recurse)))

    def store_internal_state(self, agent_id, state):
        agent_name = self.agentnames[str(agent_id)]
        log.debug("STORE_INTSTATE: %s", agent_name)
        if self.store.STORES_INTSTATE:
            self.store.store_agent_state(agent_name, str(state))

    def load_internal_state(self, agent_id):
        agent_name = self.agentnames[str(agent_id)]
        log.debug("LOAD_INTSTATE: %s", agent_name)
        if self.store.STORES_INTSTATE:
            return self.store.load_agent_state(agent_name)
        return ""

    def request_processing(self, agent_id, desc_domain, selector, targets):
        log.debug("REQUEST_PROCESSING: %s %s:%s targets %s", agent_id,
                  desc_domain, selector, [str(t) for t in targets])

        d = self.store.get_descriptor(str(desc_domain), str(selector),
                                      serialized=False)
        self.userrequestid += 1

        self.targeted_descriptor(agent_id, desc_domain, d.uuid, selector,
                                 targets, self.userrequestid)

    def new_descriptor(self, sender_id, desc_domain, uuid, selector):
        args = locals()
        args.pop('self', None)
        self.send_signal("new_descriptor", args)
        
    def targeted_descriptor(self, sender_id, desc_domain, uuid, selector,
                            targets, user_request):
        """
        Signal sent when a descriptor is sent to some target agents (not
        broadcast).
        Useful for:

        * Forcefully replaying a descriptor (debug purposes, or user request)
        * Feeding descriptors to a new agent. Used when resuming the bus.
        * Interactive mode - user may choose which selectors get send to each
          agent

        :param sender_id: sender id
        :param desc_domain: descriptor domain
        :param uuid: descriptor uuid
        :param selector: descriptor selector
        :param targets: list of target agent names. Agents not in this list
          should ignore this descriptor.
        :param user_request: True if this is a user request targeting agents
          running in interactive mode.
        """
        args = locals()
        args.pop('self', None)
        self.send_signal("targeted_descriptor", args)
    
    def bus_exit(self, awaiting_internal_state):
        """
        Signal sent when the bus is exiting.
        :param awaiting_internal_state: indicates whether agents must send
        their internal serialized state for storage.
        """
        args = locals()
        args.pop('self', None)
        self.send_signal("bus_exit", args)
        
        self.exiting = True
        return

    def on_idle(self):
        """
        Signal sent when the bus is idle, i.e. all descriptors have been
        marked as processed or processable by agents.
        """
        args = locals()
        args.pop('self', None)
        self.send_signal("on_idle", args)

    @classmethod
    def run(cls, store, server_addr):
        
        svc = cls(store, server_addr)
        log.info("Entering main loop.")
        try:
            svc.channel.start_consuming()
        except (KeyboardInterrupt, SystemExit):
            if len(svc.clients) > 0:
                log.info("Trying to stop all agents properly. Press Ctrl-C "
                         "again to stop.")
                #Ask slave agents to shutdown nicely & save internal state
                log.info("Expecting %u more agents to exit (ex. %s)",
                         len(svc.clients), svc.clients.keys()[0])
                svc.channel.queue_delete(queue='registration_queue')
                svc.bus_exit(store.STORES_INTSTATE)
                store.store_state()
                try:
                    svc.channel.start_consuming()
                except (KeyboardInterrupt, SystemExit):
                    if len(svc.clients) > 0:
                        log.info("Not all agents have stopped, exiting")

        svc.channel.cancel()
        svc.channel.close()
        svc.connection.close()

        log.info("Stopping storage...")
        store.store_state()

    @staticmethod
    def sigterm_handler(sig, frame):
        # Try to exit cleanly the first time; if that does not work, exit.
        sys.exit(0)
