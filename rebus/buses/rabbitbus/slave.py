import os
import sys
import signal
import logging
import thread
import time
import uuid as m_uuid
import pika
from rebus.agent import Agent
from rebus.bus import Bus, DEFAULT_DOMAIN
from rebus.descriptor import Descriptor
import rebus.tools.serializer as serializer


log = logging.getLogger("rebus.bus.rabbitbus")
DEFAULT_BUS = "(local dbus instance)"


@Bus.register
class RabbitBus(Bus):
    _name_ = "rabbit"
    _desc_ = "Use RabbitMQ to exchange messages by connecting to REbus master"

    # Bus methods implementations - same order as in bus.py
    def __init__(self, options):
        Bus.__init__(self)
        busaddr = options.rabbitaddr

        # Connects to the rabbitmq server
        busaddr += "/%2F?connection_attempts=200&heartbeat_interval=" +\
            str(options.heartbeat)
        self.busaddr = busaddr
        params = pika.URLParameters(busaddr)
        log.info("Connecting to rabbitmq server at: " + str(busaddr))
        b = False
        while not b:
            try:
                self.connection = pika.BlockingConnection(params)
                b = True
            except pika.exceptions.ConnectionClosed:
                log.warning("Cannot connect to rabbitmq at: " + str(busaddr) +
                            ". Retrying..")
                time.sleep(0.5)
            # TODO: quit here (failed to connect)

        self.channel = self.connection.channel()

        signal.signal(signal.SIGTERM, self.sigterm_handler)

        #: Contains agent instance. This Bus implementation accepts only one
        #: agent. Agent must be run using separate RabbitBus() (bus slave)
        #: instances.
        self.agent = None
        self.main_thread_id = thread.get_ident()

    # TODO: check if key exists
    def signal_handler(self, ch, method, properties, body):
        f = {'new_descriptor': self.broadcast_wrapper,
             'targeted_descriptor': self.targeted_wrapper,
             'bus_exit': self.bus_exit_handler,
             'on_idle': self.agent.on_idle}
        signal_type = serializer.loads(body)
        f[signal_type['signal_name']](**signal_type['args'])

    def reconnect(self):
        b = False
        params = pika.URLParameters(self.busaddr)
        while not b:
            try:
                log.info("Connecting to rabbitmq server at: " +
                         str(self.busaddr))
                self.connection = pika.BlockingConnection(params)
                self.channel = self.connection.channel()

                self.queue_ret = self.channel.queue_declare(self.return_queue)
                self.return_queue = self.queue_ret.method.queue

                self.signal_exchange = self.channel.exchange_declare(
                    exchange='rebus_signals',
                    type='fanout')
                self.ret_signal_queue = self.channel.queue_declare(
                    self.signal_queue, exclusive=True)
                self.signal_queue = self.ret_signal_queue.method.queue
                self.channel.queue_bind(exchange='rebus_signals',
                                        queue=self.signal_queue)
                self.channel.basic_consume(self.signal_handler,
                                           queue=self.signal_queue,
                                           no_ack=True)
                b = True
            except pika.exceptions.ConnectionClosed:
                log.info("Failed to reconnect to RabbitMQ. Retrying..")
                time.sleep(0.5)

    def send_rpc(self, func_name, args, high_priority=True):
        # TODO catch any exception derived from pika.exceptions.AMQPError
        # Call the remote function
        body = serializer.dumps({'func_name': func_name, 'args': args})
        corr_id = str(m_uuid.uuid4())
        routing_key = 'rebus_master_rpc_highprio' if high_priority \
            else 'rebus_master_rpc_lowprio'
        b = False
        while not b:
            try:
                retpublish = self.channel.basic_publish(
                    exchange='',
                    routing_key=routing_key,
                    body=body,
                    properties=pika.BasicProperties(reply_to=self.return_queue,
                                                    correlation_id=corr_id,))
                b = True
            except pika.exceptions.ConnectionClosed:
                log.info("Disconnected. Trying to reconnect")
                self.reconnect()

        # Wait for the return value
        response = None
        b = False
        while not b:
            try:
                meth, props, resp = self.channel.basic_get(self.return_queue)
            except pika.exceptions.ConnectionClosed:
                log.info("Disconnected. Trying to reconnect")
                self.reconnect()
            if meth:
                if corr_id == props.correlation_id:
                    b = True
                    response = str(resp)
                    response = serializer.loads(response)
                    self.channel.basic_ack(delivery_tag=meth.delivery_tag)
                    # TODO try/reconnect + break if basic_ack impossible
                else:
                    log.warning("An RPC returned with a wrong correlation ID")
            else:
                time.sleep(0.001)
        return response

    def rpc_register(self, agent_id, agent_domain, pth, config_txt):
        args = {'agent_id': agent_id, 'agent_domain': agent_domain,
                'pth': pth, 'config_txt': config_txt}
        return self.send_rpc("register", args)

    def rpc_unregister(self, agent_id):
        args = {'agent_id': agent_id}
        return self.send_rpc("unregister", args)

    def rpc_lock(self, agent_id, lockid, desc_domain, selector):
        args = {'agent_id': agent_id, 'lockid': lockid,
                'desc_domain': desc_domain, 'selector': selector}
        return self.send_rpc("lock", args)

    def rpc_unlock(self, agent_id, lockid, desc_domain, selector,
                   processing_failed, retries, wait_time):
        args = {'agent_id': agent_id, 'lockid': lockid,
                'desc_domain': desc_domain, 'selector': selector,
                'processing_failed': processing_failed, 'retries': retries,
                'wait_time': wait_time}
        return self.send_rpc("unlock", args)

    def rpc_push(self, agent_id, descriptor):
        args = {'agent_id': agent_id, 'serialized_descriptor': descriptor}
        return self.send_rpc("push", args, False)

    def rpc_get(self, agent_id, desc_domain, selector):
        args = {'agent_id': agent_id, 'desc_domain': desc_domain,
                'selector': selector}
        return self.send_rpc("get", args)

    def rpc_get_value(self, agent_id, desc_domain, selector):
        # often called from Descriptor, which does not have a reference to the
        # agent, and cannot put the correct agent_id => override agent_id
        args = {'agent_id': self.agent.id, 'desc_domain': desc_domain,
                'selector': selector}
        return self.send_rpc("get_value", args)

    def rpc_list_uuids(self, agent_id, desc_domain):
        args = {'agent_id': agent_id, 'desc_domain': desc_domain}
        return self.send_rpc("list_uuids", args)

    def rpc_find(self, agent_id, desc_domain, selector_regex, limit=0,
                 offset=0):
        args = {'agent_id': agent_id, 'desc_domain': desc_domain,
                'selector_regex': selector_regex, 'limit': limit, 'offset':
                offset}
        return self.send_rpc("find", args)

    def rpc_find_by_selector(self, agent_id, desc_domain, selector_pref,
                             limit=0, offset=0):
        args = {'agent_id': agent_id, 'desc_domain': desc_domain,
                'selector_prefix': selector_pref, 'limit': limit, 'offset':
                offset}
        return self.send_rpc("find_by_selector", args)

    def rpc_find_by_uuid(self, agent_id, desc_domain, uuid):
        args = {'agent_id': agent_id, 'desc_domain': desc_domain,
                'uuid': uuid}
        return self.send_rpc("find_by_uuid", args)

    def rpc_find_by_value(self, agent_id, desc_domain, selector_prefix,
                          value_regex):
        args = {'agent_id': agent_id, 'desc_domain': desc_domain,
                'selector_prefix': selector_prefix, 'value_regex': value_regex}
        return self.send_rpc("find_by_value", args)

    def rpc_mark_processed(self, agent_id, desc_domain, selector):
        args = {'agent_id': agent_id, 'desc_domain': desc_domain,
                'selector': selector}
        return self.send_rpc("mark_processed", args)

    def rpc_mark_processable(self, agent_id, desc_domain, selector):
        args = {'agent_id': agent_id, 'desc_domain': desc_domain,
                'selector': selector}
        return self.send_rpc("mark_processable", args)

    def rpc_get_processable(self, agent_id, desc_domain, selector):
        args = {'agent_id': agent_id, 'desc_domain': desc_domain,
                'selector': selector}
        return self.send_rpc("get_processable", args)

    def rpc_list_agents(self, agent_id):
        args = {'agent_id': agent_id}
        return self.send_rpc("list_agents", args)

    def rpc_processed_stats(self, agent_id, desc_domain):
        args = {'agent_id': agent_id, 'desc_domain': desc_domain}
        return self.send_rpc("processed_stats", args)

    def rpc_get_children(self, agent_id, desc_domain, selector, recurse):
        args = locals()
        args.pop('self', None)
        return self.send_rpc("get_children", args)

    def rpc_store_internal_state(self, agent_id, state):
        args = locals()
        args.pop('self', None)
        return self.send_rpc("store_internal_state", args)

    def rpc_load_internal_state(self, agent_id):
        args = locals()
        args.pop('self', None)
        return self.send_rpc("load_internal_state", args)

    def rpc_request_processing(self, agent_id, desc_domain, selector, targets):
        args = locals()
        args.pop('self', None)
        return self.send_rpc("request_processing", args)

    def join(self, agent, agent_domain=DEFAULT_DOMAIN):
        self.agent = agent
        self.objpath = os.path.join("/agent", self.agent.name)

        # Prefetch only 1 message from the queues at a time
        self.channel.basic_qos(prefetch_count=1)

        # Declare the registration queue to start trying to register
        self.channel.queue_declare(queue="registration_queue")

        # Fetch an ID from the ID queue
        method = False
        while not method:
            method, props, body = self.channel.basic_get(
                queue="registration_queue")
            if not method:
                self.connection.sleep(0.5)

        self.agent_id = self.agent.name + '-' + str(body)

        # Acknowledge good reception of the ID
        self.channel.basic_ack(delivery_tag=method.delivery_tag)

        # Declare RPC return queue
        ret_rpc_queue_name = "rpc_ret_" + str(self.agent_id)
        self.queue_ret = self.channel.queue_declare(
            queue=ret_rpc_queue_name, exclusive=True)
        self.return_queue = self.queue_ret.method.queue

        # Declare the signal exchange and bind the signal queue on it
        self.signal_exchange = self.channel.exchange_declare(
            exchange='rebus_signals', type='fanout')

        signal_queue_name = "signal_" + str(self.agent_id)
        self.ret_signal_queue = self.channel.queue_declare(
            queue=signal_queue_name, exclusive=True)
        self.signal_queue = self.ret_signal_queue.method.queue
        self.channel.queue_bind(exchange='rebus_signals',
                                queue=self.signal_queue)

        # Register into the bus
        self.rpc_register(self.agent_id, agent_domain, self.objpath,
                          self.agent.config_txt)

        log.info("Agent %s registered with id %s on domain %s",
                 self.agent.name, self.agent_id, agent_domain)

        return self.agent_id

    def lock(self, agent_id, lockid, desc_domain, selector):
        return bool(self.rpc_lock(str(agent_id), lockid, desc_domain,
                                  selector))

    def unlock(self, agent_id, lockid, desc_domain, selector,
               processing_failed, retries, wait_time):
        self.rpc_unlock(str(agent_id), lockid, desc_domain,
                        selector, processing_failed, retries, wait_time)

    def push(self, agent_id, descriptor):
        if thread.get_ident() == self.main_thread_id:
            self._push(str(agent_id), descriptor)
        else:
            self.busthread_call(self._push, str(agent_id), descriptor)

    def _push(self, agent_id, descriptor):
        sd = descriptor.serialize(serializer)
        return bool(self.rpc_push(str(agent_id), sd))

    def get(self, agent_id, desc_domain, selector):
        result = str(self.rpc_get(str(agent_id), desc_domain, selector))
        if result == "":
            return None
        return Descriptor.unserialize(serializer, result, bus=self)

    def get_value(self, agent_id, desc_domain, selector):
        result = str(self.rpc_get_value(str(agent_id), desc_domain, selector))
        if result == "":
            return None
        return Descriptor.unserialize_value(serializer, result)

    def list_uuids(self, agent_id, desc_domain):
        return {str(k): v.encode('utf-8') for k, v in
                self.rpc_list_uuids(str(agent_id), desc_domain).items()}

    def find(self, agent_id, desc_domain, selector_regex, limit=0, offset=0):
        slist = self.rpc_find(str(agent_id), desc_domain, selector_regex,
                              limit, offset)
        return [str(i) for i in slist]

    def find_by_selector(self, agent_id, desc_domain, selector_prefix, limit=0,
                         offset=0):
        dlist = self.rpc_find_by_selector(
            str(agent_id), desc_domain, selector_prefix, limit, offset)
        return [Descriptor.unserialize(serializer, str(s), bus=self) for s in
                dlist]

    def find_by_uuid(self, agent_id, desc_domain, uuid):
        dlist = self.rpc_find_by_uuid(str(agent_id), desc_domain, uuid)
        return [Descriptor.unserialize(serializer, str(s), bus=self) for s in
                dlist]

    def find_by_value(self, agent_id, desc_domain, selector_prefix,
                      value_regex):
        dlist = self.rpc_find_by_value(
            str(agent_id), desc_domain, selector_prefix, value_regex)
        return [Descriptor.unserialize(serializer, str(s), bus=self) for s in
                dlist]

    def mark_processed(self, agent_id, desc_domain, selector):
        self.rpc_mark_processed(str(agent_id), desc_domain, selector)

    def mark_processable(self, agent_id, desc_domain, selector):
        self.rpc_mark_processable(str(agent_id), desc_domain, selector)

    def get_processable(self, agent_id, desc_domain, selector):
        return [(str(agent_name), str(config_txt)) for (agent_name, config_txt)
                in self.rpc_get_processable(str(agent_id), desc_domain,
                                            selector)]

    def list_agents(self, agent_id):
        return {str(k): int(v) for k, v in
                self.rpc_list_agents(str(agent_id)).items()}

    def processed_stats(self, agent_id, desc_domain):
        stats, total = self.rpc_processed_stats(str(agent_id), desc_domain)
        return [(str(k), int(v)) for k, v in stats], int(total)

    def get_children(self, agent_id, desc_domain, selector, recurse=True):
        return [Descriptor.unserialize(serializer, str(s), bus=self) for s in
                self.rpc_get_children(str(agent_id), desc_domain, selector,
                                      recurse)]

    def store_internal_state(self, agent_id, state):
        self.rpc_store_internal_state(str(agent_id), state)

    def load_internal_state(self, agent_id):
        return str(self.rpc_load_internal_state(str(agent_id)))

    def request_processing(self, agent_id, desc_domain, selector, targets):
        self.rpc_request_processing(str(agent_id), desc_domain, selector,
                                    targets)

    def busthread_call(self, method, *args):
        f = lambda: method(*args)
        self.connection.add_timeout(0, f)

    def run_agents(self):
        self._run_agents()
        for args in self.agent.held_locks:
            self.agent.unlock(*args)
        # Unregister the agent before quitting
        log.debug("Unregistering...")
        self.rpc_unregister(self.agent_id)
        self.agent.save_internal_state()
        self.channel.close()
        self.connection.close()

    def _run_agents(self):
        self.agent.run_and_catch_exc()
        if self.agent.__class__.run != Agent.run:
            # the run() method has been overridden - agent will run on his own
            # then quit
            return
        try:

            self.channel.basic_consume(self.signal_handler,
                                       queue=self.signal_queue,
                                       no_ack=True)
            log.info("Entering agent loop")
            b = False
            while not b:
                try:
                    while self.channel._consumer_infos:
                        self.channel.connection.process_data_events(
                            time_limit=0.1)
                    b = True
                except pika.exceptions.ConnectionClosed:
                    log.info("Disconnected. Trying to reconnect")
                    self.reconnect()

        except (KeyboardInterrupt, SystemExit):
            log.info('Exiting...')

    def broadcast_wrapper(self, sender_id, desc_domain, uuid, selector):
        self.agent.on_new_descriptor(str(sender_id), str(desc_domain),
                                     str(uuid), str(selector), 0)

    def targeted_wrapper(self, sender_id, desc_domain, uuid, selector, targets,
                         user_request):
        if self.agent.name in targets:
            self.agent.on_new_descriptor(str(sender_id), str(desc_domain),
                                         str(uuid), str(selector),
                                         int(user_request))

    def bus_exit_handler(self, awaiting_internal_state):
        if awaiting_internal_state:
            self.agent.save_internal_state()
        self.channel.stop_consuming()

    @staticmethod
    def sigterm_handler(sig, frame):
        log.info("Caught Sigterm, unregistering and exiting.")
        sys.exit(0)

    def agent_process(self, agent, *args, **kargs):
        self.agent.call_process(*args, **kargs)

    def sleep(self, t):
        self.connection.sleep(t)

    @staticmethod
    def add_arguments(subparser):
        subparser.add_argument(
            "--rabbitaddr", default="amqp://localhost",
            help="URL prefix (scheme+authority) of the rabbitmq server")
        subparser.add_argument(
            "--heartbeat", help="Rabbitmq heartbeat interval, in seconds",
            default=0)
