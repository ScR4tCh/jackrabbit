import time

import jackrabbit
import logging
import sys
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", stream=sys.stdout)
logging.getLogger('pika').level = logging.WARN
logging.getLogger('jackrabbit').level = logging.DEBUG

jr = jackrabbit.JackRabbit('foo', 'localhost', '/', 'rabbit', 'rabbit')


@jr.consume('foo.bar')
def foo_consume(route, jackrabbit, msg):
    print "foo %s -> %s" % (route, msg)

@jr.consume('foo.bar.baz')
def foo2_consume(route, jackrabbit, msg):
    print "foo2 %s -> %s" % (route, msg)

@jr.consume('foo.bar.*')
def foo3_consume(route, jackrabbit, msg):
    print "foo3 %s -> %s" % (route, msg)

@jr.consume('foo.bar.baz.<id:int>')
def foo4_consume(route, jackrabbit, msg, id):
    print "foo4 %s -> %s for %s" % (route, msg, id)

if __name__ == '__main__':
    while(True):
        time.sleep(0.2)