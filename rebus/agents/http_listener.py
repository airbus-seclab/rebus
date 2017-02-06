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

    postprocessors = dict()

    def init_agent(self):
        self.gui = Application(self)
        self.gui.listen(8081)
        self.ioloop = tornado.ioloop.IOLoop.instance()
        t = threading.Thread(target=self.ioloop.start)
        t.daemon = True
        t.start()

    def selector_filter(self, selector):
        return False

    def inject(self, desc):
        self.push(desc)

    @staticmethod
    def registerPostProcessor(selector_prefix):
        """
        :param selector_prefix: selector prefix for which postprocessing should
            be performed

        Registers a method which will be called for selectors that match
        provided prefix.

        Only one postprocessing method will be called.

        The registered callback method must have the following prototype:
        callback(agent, selector, domain, label, value, start_time)

        This method may return either None, or a Descriptor object.

        The callback method will be run in the same process as the HTTPListener
        agent.
        """
        def func_wrapper(f):
            HTTPListener.postprocessors[selector_prefix] = f
            return f
        return func_wrapper


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
        URL format: /inject/sel/ector?domain=DOMAIN&label=LABEL&force_inject=1
        If selector is /auto, guess the selector type.
        domain is optional - defaults to 'default'

        force_inject is not obbeyed if a postprocessor intercepts the
        descriptor
        """
        start_time = time.time()
        label = self.get_argument('label', 'defaultlabel')
        domain = self.get_argument('domain', 'default')
        value = self.request.body
        force_inject = self.get_argument('force_inject', False)
        if force_inject != False:
            force_inject = True
        if selector == '/auto':
            selector = rebus.agents.inject.guess_selector(buf=value)
        postprocessor = None
        for (prefix, function) in HTTPListener.postprocessors.items():
            if selector.startswith(prefix):
                postprocessor = function
        if postprocessor is not None:
            desc = postprocessor(self.application.agent, selector, domain,
                                 label, value, start_time)
        else:
            if force_inject:
                create_new = Descriptor.new_with_randomhash
            else:
                create_new = Descriptor
            done = time.time()
            desc = create_new(label, selector, value, domain,
                              agent=self.application.agent._name_ + '_inject',
                              processing_time=(done-start_time))
        if desc is not None:
            self.application.agent.inject(desc)
        self.finish()
