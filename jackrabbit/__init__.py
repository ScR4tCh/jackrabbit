"""
JackRabbit decorated route defined threaded rabbitmq consumers

TODO: only BlockingConnection for now ... enable and test asyncio using SelectConnection !
"""
import logging
import time
import threading
import re
import pika
from pika.exceptions import AMQPConnectionError

logger = logging.getLogger('jackrabbit')

# simple types ... globals ... meh!
TYPES = ['int', 'string']
VAR = re.compile("<(?P<var>\\w+)(\\:(?P<type>((%s)|re\\:(?P<regex>.+)))){0,1}>" % '|'.join(TYPES))
ROUTE = re.compile("(\\w+|<.*>|\\*)")


class JackRabbit(object):
    should_run = True
    credentials = None
    conn_params = None
    reconnecting = False
    connected = False
    channel_in = None
    consumer = None
    consuming_inited = False

    reconnect_timeout = 0.5

    consumers = {}

    def __init__(self, exchange, host, vhost, user, passw, port=5672, declare_exchange=True):
        if(not self.connected):
            self.exchange = exchange
            self.connect(host, vhost, port, user, passw)

    def consume(self, routing_key):
        """
        consume decorator
        target function must have the following signature:

        fun(route, jackrabbit, msg)
        """

        # TODO: check presence of function arguments specified by vars in advance ?!?
        def decorate(fn):
            routing = prepare_routing_key(routing_key)
            consumer = Consumer(
                self,
                lambda channel, method, props, body, rt=routing: _wrap_callback(fn, self, rt, method, props, body),
                routing,
                True, True
            )
            self.register_consumer(routing_key, consumer)
            return fn

        return decorate

    def init_exch(self):
        try:
            self.channel_in.exchange_declare(exchange=self.exchange, durable=False, exchange_type='topic')
        except:
            # delete and retry !
            logger.info('re-declaring exchange %s' % self.exchange)
            self.channel_in = self.conn.channel()
            self.channel_in.exchange_delete(self.exchange)
            self.channel_in.exchange_declare(exchange=self.exchange, durable=False, exchange_type='topic')

    def _connect(self):
        logger.info('connecting ...')
        if(not self.reconnecting):
            self.reconnecting = True
        try:
            if(self.channel_in):
                self.channel_in.stop_consuming()

            self.conn = pika.BlockingConnection(self.conn_params)
            self.channel_in = self.conn.channel()
            self.reconnecting = False
            self.connected = True
            self.init_exch()
            self.init_queues()

            # re-init attached consumers TODO: TEST !
            for k, v in self.consumers.items():
                for i in range(0, len(v)):
                    consumer = v[i]
                    #logger.info('activation state of consumer %s : %s -> %s' % (k, consumer, 'active' if consumer.active else 'inactive'))
                    #if (not consumer.active):
                    try:
                        _consumer = consumer.renew()
                        self.consumers[k][i] = _consumer
                        _consumer.start()
                        logger.info('renewed consumer %s : %s' % (k, _consumer))
                        #consumer.join()
                        del (consumer)
                    except Exception as e:
                        logger.warn('was not able to renew consumer %s -> %s' % (k, consumer))

            #self.consumer = Consumer(self, self.channel_in, self.receive)
        except Exception as e:
            logger.warn('unable to establish connection [%s]' % e)
            time.sleep(self.reconnect_timeout)  # wait a bit before retry
            self._connect()

    def connect(self, host, vhost, port, user, passw):
        self.credentials = pika.PlainCredentials(user, passw)
        self.conn_params = pika.ConnectionParameters(host=host, port=port, virtual_host=vhost,
                                                     credentials=self.credentials, heartbeat_interval=0)
        self._connect()

    def init_queues(self):
        self.channel_in.queue_declare(queue=self.exchange+"_in", durable=True, auto_delete=False)

    def register_consumer(self, routing_key, consumer):
        if(not routing_key in self.consumers):
            self.consumers[routing_key] = []

        if(not consumer in  self.consumers[routing_key]):
            self.consumers[routing_key].append(consumer)
            logger.info('registered consumer for topic %s : %s' % (routing_key, consumer))

    def publish(self, msg, routing_key=None, content_type=None):
        msg_props = pika.BasicProperties(content_type=content_type)

    def receive(self, channel, method, props, body):
        try:
            # ack (amqp) message reception
            channel.basic_ack(delivery_tag=method.delivery_tag)
            self.consume(body)
        except AMQPConnectionError:
            logger.warn('reconnecting to rmq')
            self._connect()


# TODO: durable queues: how to clean up ?!?
class Consumer(threading.Thread):
    transport = None
    channel = None
    callback = None
    routing_key = None
    queue = None
    active = False
    callback_args = {}

    def __init__(self, jackrabbit, callback, routing_key=None, declareq=False, autostart=True, callback_args=None):
        super(Consumer, self).__init__(target=self.consume)
        self.jackrabbit = jackrabbit
        self.channel = jackrabbit.channel_in
        self.callback = callback
        self.routing_key, self.route_check = routing_key if routing_key else ('#', None)
        self.declareq = declareq
        if(autostart):
            self.start()

        if(callback_args):
            self.callback_args.update(callback_args)

    def init_consume(self):
        if(not self.declareq):
            self.queue = self.jackrabbit.exchange
            self.channel.queue_bind(queue=self.queue, exchange=self.jackrabbit.exchange, routing_key=self.routing_key)
        else:
            self.queue = self.routing_key
            self.channel.queue_declare(queue=self.routing_key, durable=True, auto_delete=False)
            self.channel.queue_bind(queue=self.routing_key, exchange=self.jackrabbit.exchange)

        self.channel.basic_qos(prefetch_count=1)

    def consume(self):
        try:
            self.init_consume()
            self.channel.basic_consume(self.callback, self.queue, arguments=self.callback_args)
            self.active = True
            # TODO: this might cause trouble if queues are already there and pumping messages (start consuming might be called multiple times
            if(not self.jackrabbit.consuming_inited):
                self.channel.start_consuming()
                self.jackrabbit.consuming_inited = True
            self.active = False
        except AMQPConnectionError:
            logger.warn('consumer disconnected')
            self.active = False
            self.jackrabbit._connect()
        except IOError:
            logger.warn('I/O failure')
            self.active = False
            self.jackrabbit._connect()
        self.active = False

    def renew(self, autostart=False):
        return Consumer(self.jackrabbit, self.callback, (self.routing_key, self.route_check),
                        self.declareq, autostart, self.callback_args)


# TODO: add simpler possebility to add custom "variable"-types
def create_check_fn(match):
    var = match.group('var')
    t = match.group('type')
    regex = match.group('regex')

    # most likely always "correct"
    if(t == 'string'):
        return lambda inpt: True
    elif(t == 'int'):
        return lambda inpt: inpt.isdigit()
    elif(t.startswith('re:') and regex):
        return lambda inpt, rex=re.compile(regex): rex.match(inpt) is not None
    else:
        return lambda inpt: False


def create_conv_fn(match):
    var = match.group('var')
    t = match.group('type')

    # most likely always "correct"
    if(t == 'int'):
        return lambda inpt: int(inpt)
    elif(t == 'string' or t.startswith('re:')):
        return lambda inpt: str(inpt)
    else:
        return lambda inpt: inpt


def prepare_routing_key(routing_key):
    ret = []
    rkey = []
    for r in ROUTE.findall(routing_key):
        m = VAR.match(r)
        ret += [(m.group('var'), create_check_fn(m), create_conv_fn(m)) if m else None]
        rkey += ['*' if m else r]

    logger.debug('route %s prepared to consume all matching %s' % (routing_key, '.'.join(rkey)))
    return '.'.join(rkey), ret


def _wrap_callback(fun, jackrabbit, routing, method, props, body):
    route = method.routing_key.split('.')
    routing_key, routing_check = routing
    args = {}

    if(any(routing_check)):
        logger.debug('perform var routing check for %s with %s' % routing)
        rc = zip(routing_check, route)
        for ck, rt in rc:
            if (ck):
                if(not ck[1](rt)):
                    jackrabbit.channel_in.basic_ack(delivery_tag=method.delivery_tag)
                    logger.debug("check %s: %s - failed" % (ck[0], rt))
                    return (None, None)
                logger.debug("check %s: %s - passed" % (ck[0], rt))
                # finally convert according to data-type and add to args
                args[ck[0]] = ck[2](rt)

    amqp_ack = False

    try:
        jackrabbit.channel_in.basic_ack(delivery_tag=method.delivery_tag)
        amqp_ack = True

        ret = fun(route=route, msg=body, jackrabbit=jackrabbit, **args)
        ret, cond = ret or (ret, None)

        logger.debug('consumer callback %s -> %s' % (method.routing_key, fun.__name__))

        return ret
    except Exception as exc:
        logger.warn('error while processing message %s -> %s  : %s' % (method.routing_key, body, str(exc)))

    if(not amqp_ack):
        jackrabbit.channel_in.basic_ack(delivery_tag=method.delivery_tag)
