#! /usr/bin/env python

import sys
import time
import signal
from collections import Counter, defaultdict
from rebus.descriptor import Descriptor
import logging
from rebus.tools.config import get_output_altering_options
import pika
import rebus.tools.serializer as serializer
from rebus.busmaster import BusMaster
from rebus.tools.sched import Sched

log = logging.getLogger("rebus.bus")


@BusMaster.cls_register
class RabbitBusMaster(BusMaster):
    _name_ = "rabbit"
    _desc_ = "Use RabbitMQ to exchange messages"

    def __init__(self, store, server_addr, heartbeat_interval=0):
        self.store = store
        #: maps agentid (ex. inject-:1.234) to object path (ex:
        #: /agent/inject)
        self.clients = {}
        self.exiting = False
        #: locks[domain] is a set of (lockid, selector) whose processing
        #: has started (might even be finished). Allows several agents that
        #: perform the same stateless computation to run in parallel
        self.locks = defaultdict(set)
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
        #: uniq_conf_clients[(agent_name, config_txt)] = [agent_id, ...]
        self.uniq_conf_clients = defaultdict(list)
        #: retry_counters[(agent_name, config_txt, domain, selector)] = \
        #:     number of remaining retries
        self.retry_counters = defaultdict(dict)
        self.sched = Sched(self._sched_inject)
        #: last published agent id
        self.last_published_id = 0

        # Connects to the rabbitmq server
        self.server_addr = (
            server_addr + "/%2F?connection_attempts=200&heartbeat_interval=" +
            str(heartbeat_interval))
        self.params = pika.URLParameters(self.server_addr)

        b = False
        while not b:
            try:
                self.connection = pika.BlockingConnection(self.params)
                b = True
            except pika.exceptions.ConnectionClosed:
                log.warning("Cannot connect to rabbitmq at: %s. Retrying...",
                            self.server_addr)
                time.sleep(0.5)

        self.channel = self.connection.channel()

        # Create the registration queue
        self.channel.queue_declare(queue="registration_queue")
        self.channel.queue_purge(queue="registration_queue")
        # Create the exchange for signals publish(master)/subscribe(slave)
        self.signal_exchange = self.channel.exchange_declare(
            exchange='rebus_signals', type='fanout')

        # Create the rpc queue
        self.channel.queue_declare(queue='rebus_master_rpc_highprio')
        self.channel.queue_purge(queue='rebus_master_rpc_highprio')
        self.channel.basic_consume(self.rpc_callback,
                                   queue='rebus_master_rpc_highprio',
                                   arguments={'x-priority': 1})
        self.channel.queue_declare(queue='rebus_master_rpc_lowprio')
        self.channel.queue_purge(queue='rebus_master_rpc_lowprio')
        self.channel.basic_consume(self.rpc_callback,
                                   queue='rebus_master_rpc_lowprio',
                                   arguments={'x-priority': 0})
        # bus is now ready to serve requests, publish registration IDs
        self.publish_ids(10000)

    def publish_ids(self, amount):
        for i in range(self.last_published_id, self.last_published_id+amount):
            self.channel.basic_publish(
                exchange="", routing_key="registration_queue", body=str(i),
                properties=pika.BasicProperties(delivery_mode=2,))
        self.last_published_id += amount


    def send_signal(self, signal_name, args):
        # Send a signal on the exchange
        body = {'signal_name': signal_name, 'args': args}
        body = serializer.dumps(body)
        b = False
        while not b:
            try:
                self.channel.basic_publish(
                    exchange='rebus_signals', routing_key='', body=body,
                    properties=pika.BasicProperties(delivery_mode=2,))
                b = True
            except pika.exceptions.ConnectionClosed:
                log.info("Disconnected (in send_signal). "
                         "Trying to reconnect...")
                self.reconnect()
                time.sleep(0.5)

    # TODO Check is the key is valid
    def call_rpc_func(self, name, args):
        f = {'register': self.register,
             'unregister': self.unregister,
             'lock': self.lock,
             'unlock': self.unlock,
             'push': self.push,
             'get': self.get,
             'get_value': self.get_value,
             'list_uuids': self.list_uuids,
             'find': self.find,
             'find_by_uuid': self.find_by_uuid,
             'find_by_selector': self.find_by_selector,
             'find_by_value': self.find_by_value,
             'mark_processed': self.mark_processed,
             'mark_processable': self.mark_processable,
             'get_processable': self.get_processable,
             'list_agents': self.list_agents,
             'processed_stats': self.processed_stats,
             'get_children': self.get_children,
             'store_internal_state': self.store_internal_state,
             'load_internal_state': self.load_internal_state,
             'request_processing': self.request_processing,
             }
        return f[name](**args)

    def rpc_callback(self, ch, method, properties, body):
        # Parse the rpc request
        body = serializer.loads(body)

        func_name = body['func_name']
        args = body['args']

        # Call the function
        ret = self.call_rpc_func(func_name, args)
        ret = serializer.dumps(ret)

        # Push the result of the function on the return queue
        b = False
        while not b:
            try:
                retpublish = ch.basic_publish(
                    exchange='',
                    routing_key=properties.reply_to,
                    body=ret,
                    properties=pika.BasicProperties(
                        correlation_id=properties.correlation_id))
                b = True
            except pika.exceptions.ConnectionClosed:
                log.info("Disconnected (in rpc_callback). Trying to reconnect")
                self.reconnect()

        ch.basic_ack(delivery_tag=method.delivery_tag)

    def update_check_idle(self, agent_name, output_altering_options):
        """
        Increases the count of handled descriptors and checks
        if all descriptors have been handled (processed/marked
        as processable).
        In that case, send the "on_idle" message.
        """
        name_config = (agent_name, output_altering_options)
        self.descriptor_handled_count[name_config] += 1
        self.check_idle()

    def check_idle(self):
        if self.exiting:
            return
        # Check if we have reached idle state
        nbdistinctagents = len(self.descriptor_handled_count)
        nbhandlings = sum(self.descriptor_handled_count.values())
        if self.descriptor_count*nbdistinctagents == nbhandlings:
            log.debug("IDLE: %d agents having distinct (name, config) %d "
                      "descriptors %d handled", nbdistinctagents,
                      self.descriptor_count, nbhandlings)
            self.on_idle()

    def register(self, agent_id, agent_domain, pth, config_txt):
        # replenish id queue
        self.publish_ids(1)
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
        self.check_idle()
        if self.exiting:
            if len(self.clients) == 0:
                log.info("Exiting - no agents are running")
                self.channel.stop_consuming()
            else:
                log.info("Expecting %u more agents to exit (ex. %s)",
                         len(self.clients), self.clients.keys()[0])

    def lock(self, agent_id, lockid, desc_domain, selector):
        objpath = self.clients[agent_id]
        locks = self.locks[desc_domain]
        key = (lockid, selector)
        log.debug("LOCK:%s %s(%s) => %r %s:%s ", lockid, objpath, agent_id,
                  key in locks, desc_domain, selector)
        if key in locks:
            return False
        locks.add(key)
        return True

    def unlock(self, agent_id, lockid, desc_domain, selector,
               processing_failed, retries, wait_time):
        objpath = self.clients[agent_id]
        locks = self.locks[desc_domain]
        lkey = (lockid, selector)
        log.debug("UNLOCK:%s %s(%s) => %r %d:%d ", lockid, objpath, agent_id,
                  processing_failed, retries, wait_time)
        if lkey not in locks:
            return
        locks.remove(lkey)
        # find agent_name, config_txt
        for (agent_name, config_txt), ids in self.uniq_conf_clients.items():
            if agent_id in ids:
                break
        rkey = (agent_name, config_txt, desc_domain, selector)
        if rkey not in self.retry_counters:
            self.retry_counters[rkey] = retries
        if self.retry_counters[rkey] > 0:
            self.retry_counters[rkey] -= 1
            desc = self.store.get_descriptor(desc_domain, selector)
            uuid = desc.uuid
            self.sched.add_action(wait_time, (agent_id, desc_domain, uuid,
                                              selector, agent_name))

    def push(self, agent_id, serialized_descriptor):
        descriptor = Descriptor.unserialize(serializer,
                                            str(serialized_descriptor))
        desc_domain = str(descriptor.domain)
        uuid = str(descriptor.uuid)
        selector = str(descriptor.selector)
        if self.store.add(descriptor):
            self.descriptor_count += 1
            log.debug("PUSH: %s => %s:%s", agent_id, desc_domain, selector)
            if not self.exiting:
                self.new_descriptor(agent_id, desc_domain, uuid, selector)
                # useful in case all agents are in idle/interactive mode
                self.check_idle()
            return True
        else:
            log.debug("PUSH: %s already seen => %s:%s", agent_id, desc_domain,
                      selector)
            return False

    def get(self, agent_id, desc_domain, selector):
        log.debug("GET: %s %s:%s", agent_id, desc_domain, selector)
        desc = self.store.get_descriptor(str(desc_domain), str(selector))
        if desc is None:
            return ""
        return desc.serialize_meta(serializer)

    def get_value(self, agent_id, desc_domain, selector):
        log.debug("GETVALUE: %s %s:%s", agent_id, desc_domain, selector)
        value = self.store.get_value(str(desc_domain), str(selector))
        if value is None:
            return ""
        return serializer.dumps(value)

    def list_uuids(self, agent_id, desc_domain):
        log.debug("LISTUUIDS: %s %s", agent_id, desc_domain)
        return self.store.list_uuids(str(desc_domain))

    def find(self, agent_id, desc_domain, selector_regex, limit=0, offset=0):
        log.debug("FIND: %s %s:%s (max %d skip %d)", agent_id, desc_domain,
                  selector_regex, limit, offset)
        return self.store.find(
            str(desc_domain), str(selector_regex), int(limit), int(offset))

    def find_by_selector(self, agent_id, desc_domain, selector_prefix, limit=0,
                         offset=0):
        log.debug("FINDBYSELECTOR: %s %s %s (max %d skip %d)", agent_id,
                  desc_domain, selector_prefix, limit, offset)
        descs = self.store.find_by_selector(
            str(desc_domain), str(selector_prefix), int(limit), int(offset))
        return [desc.serialize_meta(serializer) for desc in descs]

    def find_by_uuid(self, agent_id, desc_domain, uuid):
        log.debug("FINDBYUUID: %s %s:%s", agent_id, desc_domain, uuid)
        descs = self.store.find_by_uuid(str(desc_domain), str(uuid))
        return [desc.serialize_meta(serializer) for desc in descs]

    def find_by_value(self, agent_id, desc_domain, selector_prefix,
                      value_regex):
        log.debug("FINDBYVALUE: %s %s %s %s", agent_id, desc_domain,
                  selector_prefix, value_regex)
        descs = self.store.find_by_value(str(desc_domain),
                                         str(selector_prefix),
                                         str(value_regex))
        return [desc.serialize_meta(serializer) for desc in descs]

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
                                            serializer=serializer,
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

        d = self.store.get_descriptor(str(desc_domain), str(selector))
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

    def reconnect(self):
        b = False
        while not b:
            try:
                log.info("Re-connecting to rabbitmq server at: " +
                         str(self.server_addr))
                self.connection = pika.BlockingConnection(self.params)
                self.channel = self.connection.channel()

                self.channel.queue_declare(queue="registration_queue")
                self.signal_exchange = self.channel.exchange_declare(
                    exchange='rebus_signals', type='fanout')
                self.channel.queue_declare(queue='rebus_master_rpc_highprio')
                self.channel.basic_consume(
                    self.rpc_callback,
                    queue='rebus_master_rpc_highprio',
                    arguments={'x-priority': 1})
                self.channel.queue_declare(queue='rebus_master_rpc_lowprio')
                self.channel.basic_consume(
                    self.rpc_callback,
                    queue='rebus_master_rpc_lowprio',
                    arguments={'x-priority': 0})
                b = True
            except pika.exceptions.ConnectionClosed:
                log.info("Failed to reconnect to RabbitMQ. Retrying..")
                time.sleep(0.5)

    @classmethod
    def run(cls, store, master_options):

        server_addr = master_options.rabbitaddr
        heartbeat_interval = master_options.heartbeat
        svc = cls(store, server_addr, heartbeat_interval)
        log.info("Entering main loop.")
        try:
            while True:
                try:
                    svc.channel.start_consuming()
                except pika.exceptions.ConnectionClosed:
                    log.info("Disconnected (in run). Trying to reconnect")
                    cls.reconnect()
        except (KeyboardInterrupt, SystemExit):
            log.info("Received SIGINT or Ctrl-C, exiting")
            svc.channel.queue_delete(queue='registration_queue')
            if len(svc.clients) > 0:
                log.info("Trying to stop all agents properly. Press Ctrl-C "
                         "again to stop.")
                # stop scheduler
                svc.sched.shutdown()
                # ask slave agents to shutdown nicely & save internal state
                log.info("Expecting %u more agents to exit (ex. %s)",
                         len(svc.clients), svc.clients.keys()[0])
                svc.bus_exit(store.STORES_INTSTATE)
                store.store_state()
                try:
                    while True:
                        try:
                            svc.channel.start_consuming()
                            if len(svc.clients) == 0:
                                break
                        except pika.exceptions.ConnectionClosed:
                            log.info("Disconnected. Trying to reconnect")
                            cls.reconnect()
                except (KeyboardInterrupt, SystemExit):
                    if len(svc.clients) > 0:
                        log.info(
                            "Not all agents have stopped, exiting nonetheless")

        svc.channel.cancel()
        svc.channel.close()
        svc.connection.close()

        log.info("Stopping storage...")
        store.store_state()

    @staticmethod
    def sigterm_handler(sig, frame):
        # Try to exit cleanly the first time; if that does not work, exit.
        # raises SystemExit, caught in run()
        sys.exit(0)

    @staticmethod
    def add_arguments(subparser):
        subparser.add_argument(
            "--rabbitaddr", default="amqp://localhost",
            help="URL prefix (scheme+authority) of the rabbitmq server")
        subparser.add_argument(
            "--heartbeat", help="Rabbitmq heartbeat interval, in seconds",
            default=0)

    def busthread_call(self, method, *args):
        f = lambda: method(*args)
        self.connection.add_timeout(0, f)

    def _sched_inject(self, agent_id, desc_domain, uuid, selector, target):
        """
        Called by Sched object, from Timer thread. Emits targeted_descriptor
        through bus thread.
        """
        self.busthread_call(
            self.targeted_descriptor,
            *(agent_id, desc_domain, uuid, selector, [target], False))
