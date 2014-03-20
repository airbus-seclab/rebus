import os
from rebus.agent import Agent
import threading
import tornado.ioloop
import tornado.web
import rebus.agents.inject
from rebus.descriptor import Descriptor
import collections
import functools


@Agent.register
class WebInterface(Agent):
    _name_ = "web_interface"
    _desc_ = "Dump all descriptors exchanged on the bus to a web interface"

    def init_agent(self):
        self.dstore = DescriptorStore(self)
        self.gui = Application(self.dstore)
        self.gui.listen(8080)
        self.ioloop = tornado.ioloop.IOLoop.instance()
        t = threading.Thread(target = self.ioloop.start)
        t.daemon = True
        t.start()

    def process(self, desc, sender_id):
        self.ioloop.add_callback(self.dstore.new_descriptor, desc, sender_id)

    def get_descriptor_value(self, selector):
        dlist = self.bus.get_past_descriptors(self, selector)
        if len(dlist) > 1:
            return "More than one descriptors match this selector"
        return dlist[0]

    def inject(self, filename, buf):
        label = filename
        selector = rebus.agents.inject.guess_selector(buffer=buf)
        data = buf
        domain = label
        desc = Descriptor(label, selector, data, domain)
        self.push(desc)


class Application(tornado.web.Application):
    def __init__(self, dstore):
        handlers = [
            (r"/", tornado.web.RedirectHandler, {'url': '/monitor'}),
            (r"/monitor", MonitorHandler),
            (r"/inject", InjectHandler),
            (r"/analysis", AnalysisHandler),
            (r"/poll_descriptors", DescriptorUpdatesHandler),
            (r"/get(.*)", DescriptorGetHandler),
        ]
        params = {
            'template_path': os.path.join(os.path.dirname(__file__), 'templates'),
            'static_path': os.path.join(os.path.dirname(__file__), 'static')
        }
        self.dstore = dstore
        tornado.web.Application.__init__(self, handlers, **params)

class DescriptorStore(object):
    def __init__(self, agent):
        # self.waiters[domain] is a set of callbacks for new descriptors on domain
        # self.waiters['default'] is a set of callbacks for new descriptors on any domain
        self.waiters = collections.defaultdict(set)
        self.cache = []
        self.cache_size = 200
        self.rlock = threading.RLock()
        self.agent = agent

    def wait_for_descriptors(self, callback, cursor=None, page=None, domain='default'):
        if cursor:
            with self.rlock:
                new_count = 0
                for desc in reversed(self.cache):
                    if desc['hash'] == cursor:
                        break
                    new_count += 1
                if new_count:
                    callback(self.cache[-new_count:], page)
                    return
        self.waiters[domain].add(functools.partial(callback, page=page))

    def cancel_wait(self, callback):
        for domainset in self.waiters.values():
            try:
                domainset.remove(callback)
            except KeyError:
                pass

    def new_descriptor(self, desc, sender_id):
        agent, uniqueid = str(sender_id).rsplit('-', 1)
        printablevalue = desc.value if isinstance(desc.value, unicode) else ''
        if len(printablevalue) > 80:
            printablevalue = (printablevalue[:80] + '...')

        descrinfo = {
            'hash': desc.hash,
            'domain': desc.domain,
            'agent': agent,
            'uniqueid': uniqueid,
            'selector': desc.selector.partition('%')[0],
            'fullselector': desc.selector,
            'label': desc.label,
            'printablevalue': printablevalue,
        }
        for callback in self.waiters['default'] | self.waiters[desc.domain]:
                callback([descrinfo])
                try:
                    del self.waiters[desc.domain]
                    del self.waiters['default']
                except KeyError:
                    pass
        with self.rlock:
            self.cache.append(descrinfo)
            if len(self.cache) > self.cache_size:
                self.cache = self.cache[-self.cache_size:]
    def inject(self, filename, buffer):
        self.agent.inject(filename, buffer)

    def get_by_selector(self, s):
        return self.agent.get_descriptor_value(s).value

class AnalysisHandler(tornado.web.RequestHandler):
    def get(self):
        self.render('analysis.html')

class MonitorHandler(tornado.web.RequestHandler):
    def get(self):
        self.render('monitor.html', descriptors=self.application.dstore.cache)

class DescriptorUpdatesHandler(tornado.web.RequestHandler):
    """
    Dispatches new descriptors to web clients.
    """
    @tornado.web.asynchronous
    def post(self):
        cursor = self.get_argument('cursor', None)
        page = self.get_argument('page', None)
        domain = self.get_argument('domain', 'default')
        self.application.dstore.wait_for_descriptors(self.on_new_descriptors,
                cursor=cursor, page=page, domain=domain)

    def on_new_descriptors(self, descrinfos, page):
        # Closed client connection
        if self.request.connection.stream.closed():
            return
        # contains only data from descrinfos needed to render page
        infos = []
        with self.application.dstore.rlock:
            for d in descrinfos:
                info = {}
                infos.append(info)
                for k in ('hash', 'selector', 'fullselector', 'printablevalue'):
                    info[k] = d[k]
                if page == 'monitor':
                    for k in ('label', 'domain', 'uniqueid', 'agent'):
                        info[k] = d[k]
                if page in ('monitor', 'analysis'):
                    d['html_' + page] = self.render_string('descriptor_%s.html' % page, descriptor=d)
                    info['html'] = d['html_' + page]
        self.finish(dict(descrinfos=infos))

    def on_connection_close(self):
        self.application.dstore.cancel_wait(self.on_new_descriptors)

class DescriptorGetHandler(tornado.web.RequestHandler):
    """
    Handles requests for descriptor values.
    Values are requested through the bus.
    """
    def get(self, selector='', *args, **kwargs):
        if self.get_argument('download', '0') == '1':
            self.set_header('Content-Disposition', 'attachment; filename=%s' % tornado.escape.url_escape(selector.split('/')[-1]))
        data = self.application.dstore.get_by_selector(selector)
        if type(data) not in ['unicode', 'str']:
            data = str(data)
        self.finish(data)

class InjectHandler(tornado.web.RequestHandler):
    """
    Injects a file to the bus.
    """
    def post(self, *args, **kwargs):
        f = self.request.files['file'][0]
        self.application.dstore.inject(f['filename'], f['body'])
        self.finish('{}')

