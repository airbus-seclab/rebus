import os
from rebus.agent import Agent
import threading
import tornado.ioloop
import tornado.web
import rebus.agents.inject
from rebus.descriptor import Descriptor
from collections import OrderedDict
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
        value = buf
        domain = "default"  # TODO allow user to specify domain
        desc = Descriptor(label, selector, value, domain,
                          agent=self._name_ + '_inject')
        if not self.push(desc):
            for desc in self.bus.find_by_uuid(self, domain, desc.uuid):
                self.ioloop.add_callback(self.dstore.new_descriptor, desc,
                                         "storage-0")
        return desc.uuid


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
        self.agent = agent

        # self.waiters is a set of (domain, uuid, callback, page) for new
        # descriptors on specified uuid and domain
        # "domain" and "uuid" may be empty for callback to be called on any
        # value
        self.waiters = set()

        # Most recent descriptor is in self.cache[0]
        self.cache = []
        self.cache_size = 200

        # Used to protect access to self.cache from both threads (bus and
        # tornado)
        self.rlock = threading.RLock()

    def wait_for_descriptors(self, callback, domain, uuid, page, cursor=None):
        """
        :param callback: callback function, will be called when necessary
        :param domain: domain filter. Empty if any
        :param uuid: uuid filter. Empty if any
        :param cursor: descriptor of most recent displayed hash
        :param page: string parameter to be passed to callback()

        Registers callback to be called when a matching descriptor is received.
        """
        if cursor:
            # Immediately return matching descriptors if available
            with self.rlock:
                new_count = 0
                for desc in reversed(self.cache):
                    if desc['hash'] == cursor:
                        break
                    new_count += 1
                if new_count > 0:
                    # New descriptors are available. Send them if they match.
                    matching_descs = []
                    for desc in self.cache[-new_count:]:
                        if domain == desc['domain'] or not domain:
                            if uuid == desc['uuid'] or not uuid:
                                matching_descs.append(desc)
                    if matching_descs:
                        callback(matching_descs, page)
                        return
        # No new matching descriptors have been found, start waiting
        self.waiters.add((domain, uuid, callback, page))

    def cancel_wait(self, callback):
        for (domain, uuid, cb, page) in set(self.waiters):
            if callback == cb:
                self.waiters.remove((domain, uuid, cb, page))

    def new_descriptor(self, desc, sender_id):
        """
        :param desc: new descriptor
        :param sender_id: sender agent ID

        Callback function
        Called whenever a new descriptor is available (received from bus, or
        injected by web_interface)
        """
        agent, uniqueid = str(sender_id).rsplit('-', 1)
        printablevalue = desc.value if isinstance(desc.value, unicode) else ''
        if len(printablevalue) > 80:
            printablevalue = (printablevalue[:80] + '...')

        descrinfo = {
            'hash': desc.hash,
            'domain': desc.domain,
            'uuid': desc.uuid,
            'agent': desc.agent,
            'sender': agent,
            'uniqueid': uniqueid,
            'selector': desc.selector.partition('%')[0],
            'fullselector': desc.selector,
            'label': desc.label,
            'printablevalue': printablevalue,
            'processing_time': format(desc.processing_time, '.3f'),
        }
        for (domain, uuid, callback, page) in list(self.waiters):
            if domain == desc.domain or not domain:
                if uuid == desc.uuid or not uuid:
                    callback([descrinfo], page)
                    self.waiters.remove((domain, uuid, callback, page))
        with self.rlock:
            self.cache.append(descrinfo)
            if len(self.cache) > self.cache_size:
                self.cache = self.cache[-self.cache_size:]

    def inject(self, filename, buf):
        return self.agent.inject(filename, buf)

    def get_by_selector(self, domain, s):
        return self.agent.get_descriptor(domain, s)

    def find(self, domain, sel_regex, limit):
        return self.agent.find(domain, sel_regex, limit)


class AnalysisHandler(tornado.web.RequestHandler):
    def get(self):
        self.render('analysis.html')


class SelectorsHandler(tornado.web.RequestHandler):
    def get(self):
        sels = self.application.dstore.find(
            self.get_argument('domain', 'default'), '/.*', limit=100)
        self.render('selectors.html', selectors=sorted(sels))


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
        page = self.get_argument('page')
        domain = self.get_argument('domain')
        uuid = self.get_argument('uuid')
        self.application.dstore.wait_for_descriptors(self.on_new_descriptors,
                                                     domain, uuid, page,
                                                     cursor)

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
                    for k in ('label', 'uniqueid', 'processing_time'):
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
            self.send_error(status_code=404)
            return

        label = desc.label
        data = desc.value

        if download:
            self.set_header('Content-Disposition', 'attachment; filename=%s' %
                            tornado.escape.url_escape(label))
        if '/matrix/' in selector and not download:
            contents = OrderedDict()
            hashes = sorted(data[0].keys(), key=lambda x: data[0][x])

            # Compute colors thresholds depending on values
            values = sorted(data[1].values())

            # For merged matrix, compute average distance to determine colors
            if type(values) is list and values and type(values[0]) is dict:
                values = [x.values() for x in values]
                values = sorted(map(numpy.average, values))

            colorclasses = ['info', 'success', 'warning', 'danger']
            nbcolors = len(colorclasses)
            if len(values) > 0:
                colorthresh = [values[((i+1)*len(values))/nbcolors]
                               for i in range((nbcolors-1))]
            else:
                colorthresh = [0] * (nbcolors-1)
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
                            contents[h1name].append(
                                (avg,
                                 self.color(colorthresh, colorclasses, avg)))
                        else:
                            contents[h1name].append(
                                (str(int(sd*100))+str(value),
                                 self.color(colorthresh, colorclasses, avg)))
                    else:
                        contents[h1name].append(
                            (value,
                             self.color(colorthresh, colorclasses, value)))
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
        uuid = self.application.dstore.inject(f['filename'], f['body'])
        self.finish(dict(uuid=uuid))
