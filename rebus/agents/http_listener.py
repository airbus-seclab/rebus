from rebus.agent import Agent
import threading
import time
import tornado.ioloop
import tornado.web
import tornado.template
import rebus.agents.inject
from rebus.descriptor import Descriptor


@Agent.register
class HTTPListener(Agent):
    _name_ = "httplistener"
    _desc_ = "Push any descriptor that gets POSTed to the bus"

    def init_agent(self):
        self.gui = Application(self)
        self.gui.listen(8081)
        self.ioloop = tornado.ioloop.IOLoop.instance()
        t = threading.Thread(target=self.ioloop.start)
        t.daemon = True
        t.start()

    def selector_filter(self, selector):
        return False

    def inject(self, selector, domain, label, value, start_time):
        done = time.time()
        desc = Descriptor(label, selector, value, domain,
                          agent=self._name_ + '_inject',
                          processing_time=(done-start_time))
        self.push(desc)


class Application(tornado.web.Application):
    def __init__(self, agent):
        handlers = [
            (r"/inject(/[^\?]*)\??.*", InjectHandler),
        ]
        self.agent = agent
        tornado.web.Application.__init__(self, handlers)


class InjectHandler(tornado.web.RequestHandler):
    def post(self, selector, *args, **kwargs):
        """
        Handles POST requests. Injects POSTed values to the bus.
        URL format: /inject/sel/ector?domain=DOMAIN&label=LABEL
        If selector is /auto, guess the selector type.
        domain is optional - defaults to 'default'
        """
        start_time = time.time()
        label = self.get_argument('label')
        domain = self.get_argument('domain', 'default')
        value = self.request.body
        if selector == '/auto':
            selector = rebus.agents.inject.guess_selector(buf=value)
        self.application.agent.inject(selector, domain, label, value,
                                      start_time)
        self.finish()
