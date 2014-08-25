import os
from rebus.agent import Agent
import threading
import tornado.ioloop
import tornado.web
import rebus.agents.inject
from rebus.descriptor import Descriptor
import collections
import functools
import numpy


@Agent.register
class WebInterface(Agent):
    _name_ = "web_interface"
    _desc_ = "Dump all descriptors exchanged on the bus to a web interface"

    def init_agent(self):
        self.dstore = DescriptorStore(self)
        self.gui = Application(self.dstore)
        self.gui.listen(8080)
        self.ioloop = tornado.ioloop.IOLoop.instance()
        t = threading.Thread(target=self.ioloop.start)
        t.daemon = True
        t.start()

    def process(self, desc, sender_id):
        self.ioloop.add_callback(self.dstore.new_descriptor, desc, sender_id)

    def get_descriptor(self, domain, selector):
        desc = self.bus.get(self, domain, selector)
        return desc

    def inject(self, filename, buf):
        label = filename
        selector = rebus.agents.inject.guess_selector(buf=buf)
        data = buf
        domain = label
        desc = Descriptor(label, selector, data, domain, agent=self._name_ + '_inject')
        if not self.push(desc):
            for desc in self.bus.get_children(self, domain, desc.selector):
                self.ioloop.add_callback(self.dstore.new_descriptor, desc,
                                         "storage-0")


class Application(tornado.web.Application):
    def __init__(self, dstore):
        handlers = [
            (r"/", tornado.web.RedirectHandler, {'url': '/monitor'}),
            (r"/monitor", MonitorHandler),
            (r"/inject", InjectHandler),
            (r"/analysis", AnalysisHandler),
            (r"/selectors", SelectorsHandler),
            (r"/poll_descriptors", DescriptorUpdatesHandler),
            (r"/get(.*)", DescriptorGetHandler),
        ]
        params = {
            'template_path': os.path.join(os.path.dirname(__file__),
                                          'templates'),
            'static_path': os.path.join(os.path.dirname(__file__), 'static')
        }
        self.dstore = dstore
        tornado.web.Application.__init__(self, handlers, **params)


class DescriptorStore(object):
    def __init__(self, agent):
        # self.waiters[domain] is a set of callbacks for new descriptors on
        # domain
        # self.waiters['default'] is a set of callbacks for new descriptors on
        # any domain
        self.waiters = collections.defaultdict(set)
        self.cache = []
        self.cache_size = 200
        self.rlock = threading.RLock()
        self.agent = agent

    def wait_for_descriptors(self, callback, cursor=None, page=None,
                             domain='default'):
        if cursor:
            with self.rlock:
                new_count = 0
                for desc in reversed(self.cache):
                    if desc['hash'] == cursor:
                        break
                    new_count += 1
                if new_count:
                    if domain == 'default':
                        callback(self.cache[-new_count:], page)
                    else:
                        callback([d for d in self.cache[-new_count:] if
                                  d['domain'] == domain], page)
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
            'agent': desc.agent,
            'sender': agent,
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
                # Happens when no waiters are registered for domains 'default'
                # or desc.domain
                pass
        with self.rlock:
            self.cache.append(descrinfo)
            if len(self.cache) > self.cache_size:
                self.cache = self.cache[-self.cache_size:]

    def inject(self, filename, buf):
        self.agent.inject(filename, buf)

    def get_by_selector(self, domain, s):
        return self.agent.get_descriptor(domain, s)

    def find(self, domain, sel_regex, limit):
        return self.agent.find(domain, sel_regex, limit)


class AnalysisHandler(tornado.web.RequestHandler):
    def get(self):
        self.render('analysis.html')


class SelectorsHandler(tornado.web.RequestHandler):
    def get(self):
        sels = self.application.dstore.find(self.get_argument('domain','default'), '/.*', limit=100)
        self.render('selectors.html', selectors = sorted(sels))

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
                                                     cursor=cursor, page=page,
                                                     domain=domain)

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
                for k in ('hash', 'selector', 'fullselector', 'printablevalue',
                          'agent', 'domain'):
                    info[k] = d[k]
                if page == 'monitor':
                    for k in ('label', 'uniqueid'):
                        info[k] = d[k]
                if page in ('monitor', 'analysis'):
                    d['html_' + page] = self.render_string('descriptor_%s.html'
                                                           % page,
                                                           descriptor=d)
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
        download = (self.get_argument('download', '0') == '1')
        domain = self.get_argument('domain', 'default')
        desc = self.application.dstore.get_by_selector(domain, selector)
        if desc is None:
            send_error(status_code=404)

        label = desc.label
        data = desc.value

        if download:
            self.set_header('Content-Disposition', 'attachment; filename=%s' %
                            tornado.escape.url_escape(label))
        if '/matrix/' in selector and not download:
            contents = collections.OrderedDict()
            hashes = sorted(data[0].keys(), key=lambda x: data[0][x])

            # Compute colors thresholds depending on values
            values = sorted(data[1].values())

            # For merged matrix, compute average distance to determine colors
            if type(values) is list and type(values[0]) is dict:
                values = map(lambda x: x.values(), values)
                values =sorted(map(numpy.average, values))

            colorclasses = ['info', 'success', 'warning', 'danger']
            nbcolors = len(colorclasses)
            if len(values) > 0:
                colorthresh = [values[((i+1)*len(values))/nbcolors] for i in range((nbcolors-1))]
            else:
                colorthresh = [0] * (nbcolors-1)
            colors = dict()
            for h1 in hashes:
                h1name = data[0][h1]
                contents[h1name] = list()
                for h2 in hashes:
                    value = data[1].get(frozenset((h1, h2)), "X")
                    if type(value) is dict:
                        d = value.values()
                        avg = numpy.average(d)
                        sd = numpy.std(d)
                        if sd < 0.2:
                            contents[h1name].append((avg, self.color(colorthresh, colorclasses, value)))
                        else:
                            contents[h1name].append((str(int(sd*100))+str(value), self.color(colorthresh, colorclasses, avg)))
                    else:
                        contents[h1name].append((value, self.color(colorthresh, colorclasses, value)))
            self.render('matrix.html', matrix=contents)
        else:
            if type(data) not in [unicode, str]:
                data = str(data)
            self.finish(data)

    def color(self, colorthresh, colorclasses, value):
        if type(value) in [unicode, str]:
            return
        if value < colorthresh[0]:
            return colorclasses[0]
        for idx, t in reversed(list(enumerate(colorthresh))):
            if value >= t:
                return colorclasses[idx+1]



class InjectHandler(tornado.web.RequestHandler):
    """
    Injects a file to the bus.
    """
    def post(self, *args, **kwargs):
        f = self.request.files['file'][0]
        self.application.dstore.inject(f['filename'], f['body'])
        self.finish('{}')
