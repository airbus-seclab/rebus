import os
from rebus.agent import Agent
import threading
import tornado.ioloop
import tornado.web
import tornado.template
import rebus.agents.inject
from rebus.descriptor import Descriptor
import re
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

    def process(self, descriptor, sender_id):
        self.ioloop.add_callback(self.dstore.new_descriptor, descriptor,
                                 sender_id)

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


class CustomLoader(tornado.template.Loader):
    """
    Use parent class Loader to load any template other than descriptors.

    To render descriptor of type "desctype" (first selector part, e.g.
    "matrix"), for page "thepage", try to use
    templates/descriptor/desctype_thepage. If it does not exist, use
    templates/descriptor/default_thepage.

    Load descriptor templates either from the descriptor/ folders, or using
    agent-registered templates (allows agents that live outside the rebus
    repository to specify how to render their custom desctypes).

    To render any other type of templates (e.g. /analysis page), use static
    template files in templates/
    """

    #: contains descriptor templates that have been registered by external (out
    #: of rebus) agents, as well of static files that are part of rebus
    templates = dict()

    def __init__(self, root_directory, **kwargs):
        """
        Register existing static files
        """
        super(CustomLoader, self).__init__(root_directory, **kwargs)

        for fname in os.listdir(os.path.join(root_directory, "descriptor")):
            fullpath = os.path.join(root_directory, "descriptor", fname)
            if not (fname.endswith('.html') and os.path.isfile(fullpath)):
                continue
            # filename format: descriptor/desctype/page
            desc_type, page = fname.rsplit('.', 1)[0].rsplit('_', 1)
            CustomLoader.register(desc_type, page, open(fullpath, 'rb').read())

    @staticmethod
    def register(desc_type, page, templatestr):
        """
        Called to register a renderering template for the given page and
        descriptor type.
        """
        CustomLoader.templates[(desc_type, page)] = templatestr

    def resolve_path(self, name, parent_path=None):
        name = super(CustomLoader, self).resolve_path(name, parent_path)
        return name

    def _create_template(self, name):
        """
        Return the requested template object.
        """

        if not name.startswith('descriptor/'):
            return super(CustomLoader, self)._create_template(name)

        desc_type, page = name.rsplit('/', 1)[1].rsplit('_', 1)
        if (desc_type, page) in CustomLoader.templates:
            # try to load specific template
            templatestr = CustomLoader.templates[(desc_type, page)]
        else:
            # use default otherwise
            templatestr = CustomLoader.templates[('default', page)]
        template = tornado.template.Template(templatestr, name=name,
                                             loader=self)
        return template


class Application(tornado.web.Application):
    def __init__(self, dstore):
        handlers = [
            (r"/", tornado.web.RedirectHandler, {'url': '/monitor'}),
            (r"/monitor", MonitorHandler),
            (r"/inject", InjectHandler),
            (r"/uuid/(.*)", UUIDHandler),
            (r"/analysis(|/.*)", AnalysisHandler),
            (r"/selectors", SelectorsHandler),
            (r"/poll_descriptors", DescriptorUpdatesHandler),
            (r"/get(.*)", DescriptorGetHandler),
            (r"/agents", AgentsHandler),
        ]
        params = {
            'static_path': os.path.join(os.path.dirname(__file__), 'static'),
            'template_loader':
                CustomLoader(os.path.join(os.path.dirname(__file__),
                                          'templates'),)
        }
        self.dstore = dstore
        tornado.web.Application.__init__(self, handlers, **params)


class DescriptorStore(object):
    def __init__(self, agent):
        self.agent = agent

        #: self.waiters is a set of (domain, uuid, callback, page) for new
        #: descriptors on specified uuid and domain
        #: "domain" and "uuid" may be empty for callback to be called on any
        #: value
        self.waiters = set()

        #: Most recent descriptor is in self.cache[-1].
        #: The cache is used:
        #:
        #: * in the Bus Monitor view when a user first loads the page
        #: * in every page that updates dynamically, to cache descriptors
        #:   between two (long) pollings
        self.cache = []
        self.cache_size = 200

        #: Used to protect access to self.cache from both threads (bus and
        #: tornado)
        self.rlock = threading.RLock()

    def wait_for_descriptors(self, callback, domain, uuid, page, cursor):
        """
        :param callback: callback function, will be called when necessary
        :param domain: domain filter. Empty if any
        :param uuid: uuid filter. Empty if any
        :param cursor: 'cached', 'all', or hash of the most recent displayed
            descriptors.
        :param page: string parameter to be passed to callback()

        Returns matching descriptor information if available.
        Else, registers callback to be called when a matching descriptor is
        received.

        Usage scenarios:

        * Fetch any old descriptor matching uuid and domain. Wait if none
            match.
        * Fetch matching cached descriptors (domain and uuid may or may not be
            specified).
        """
        matching_infos = []

        if cursor == 'all':
            # Search whole bus
            # Domain or uuid must be defined
            # Wait if none have been found
            descs = []
            if domain and uuid:
                descs = self.agent.bus.find_by_uuid(self.agent, domain, uuid)
            matching_infos = self.info_from_desc(descs)

        else:
            # Return cached descriptors that are newer than cursor (all cached
            # if cursor is not in cache anymore).
            # Also works for cursor == 'cached'
            with self.rlock:
                new_count = 0
                for desc in reversed(self.cache):
                    if desc['hash'] == cursor:
                        break
                    new_count += 1
                if new_count > 0:
                    # New descriptors are available. Send them if they match.
                    for desc in self.cache[-new_count:]:
                        if domain == desc['domain'] or not domain:
                            if uuid == desc['uuid'] or not uuid:
                                matching_infos.append(desc)
                    if matching_infos:
                        callback(matching_infos, page)
                        return

        if matching_infos:
            callback(matching_infos, page)
            return
        # No new matching descriptors have been found, start waiting
        self.waiters.add((domain, uuid, callback, page))

    def info_from_desc(self, descriptors):
        """
        :param descriptors: list of descriptors

        Return a list of descriptor summary dictionnaries
        """

        descrinfos = []
        for desc in descriptors:
            printablevalue = desc.value if isinstance(desc.value, unicode) \
                else ''
            if len(printablevalue) > 80:
                printablevalue = (printablevalue[:80] + '...')

            descrinfo = {
                'hash': desc.hash,
                'domain': desc.domain,
                'uuid': desc.uuid,
                'agent': desc.agent,
                'selector': desc.selector.partition('%')[0],
                'fullselector': desc.selector,
                'label': desc.label,
                'printablevalue': printablevalue,
                'processing_time': format(desc.processing_time, '.3f'),
                'version': desc.version,
            }
            if desc.selector.startswith('/link/'):
                descrinfo['value'] = desc.value
                descrinfo['linksrchash'] = desc.value['selector'].split('%')[1]
            descrinfos.append(descrinfo)
        return descrinfos

    def cancel_wait(self, callback):
        for (domain, uuid, cb, page) in set(self.waiters):
            if callback == cb:
                self.waiters.remove((domain, uuid, cb, page))

    def new_descriptor(self, descriptor, sender_id):
        """
        :param descriptor: new descriptor
        :param sender_id: sender agent ID

        Callback function
        Called whenever a new descriptor is available (received from bus, or
        injected by web_interface)
        """
        descrinfo = self.info_from_desc([descriptor])[0]
        for (domain, uuid, callback, page) in list(self.waiters):
            if domain == descriptor.domain or not domain:
                if uuid == descriptor.uuid or not uuid:
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

    def list_uuids(self, desc_domain):
        return self.agent.list_uuids(desc_domain)

    def processed_stats(self, desc_domain):
        return self.agent.processed_stats(desc_domain)


class AnalysisHandler(tornado.web.RequestHandler):
    def get(self, uuid=''):
        """
        URL format: /analysis (blank page)
                    /analysis/domain/aaaaaaaa-1234-5678-abcd-123456789abc
        """
        if uuid not in ('', '/') and\
           not re.match('/[a-zA-Z0-9-_]+/[0-9a-fA-F-]{36}', uuid):
            # invalid uuid
            self.send_error(400)
            return

        self.render('analysis.html', uuid=uuid)


class UUIDHandler(tornado.web.RequestHandler):
    def get(self, domain):
        uuid_label = self.application.dstore.list_uuids(domain)
        self.render('uuid.html', domain=domain,
                    selectors=sorted(uuid_label.items(), key=lambda x: x[1]))


class SelectorsHandler(tornado.web.RequestHandler):
    def get(self):
        sels = self.application.dstore.find(
            self.get_argument('domain', 'default'), '/.*', limit=100)
        self.render('selectors.html', selectors=sorted(sels))


class MonitorHandler(tornado.web.RequestHandler):
    def get(self):
        self.render('monitor.html')


class DescriptorUpdatesHandler(tornado.web.RequestHandler):
    """
    Dispatches new descriptors to web clients.
    """
    @tornado.web.asynchronous
    def post(self):
        cursor = self.get_argument('cursor')
        page = self.get_argument('page')
        domain = self.get_argument('domain')
        uuid = self.get_argument('uuid')
        self.application.dstore.wait_for_descriptors(self.on_new_descriptors,
                                                     domain, uuid, page,
                                                     cursor)

    def on_new_descriptors(self, descrinfos, page):
        if self.request.connection.stream.closed():
            return

        #: Contains only data from descrinfos needed to render page
        infos = []
        with self.application.dstore.rlock:
            for d in descrinfos:
                info = {}
                infos.append(info)
                for k in ('hash', 'selector', 'fullselector', 'printablevalue',
                          'agent', 'domain', 'label', 'linksrchash',
                          'version'):
                    if k in d:
                        info[k] = d[k]
                if page == 'monitor':
                    for k in ('processing_time',):
                        info[k] = d[k]
                if page in ('monitor', 'analysis'):
                    desc_type = d['selector'].split('/')[1]
                    d['html_' + page] = self.render_string('descriptor/%s_%s'
                                                           % (desc_type, page),
                                                           descriptor=d)
                    info['html'] = d['html_' + page]
        self.finish(dict(descrinfos=infos))

    def on_connection_close(self):
        self.application.dstore.cancel_wait(self.on_new_descriptors)


class DescriptorGetHandler(tornado.web.RequestHandler):
    """
    Handles requests for descriptor values.
    Values are requested through the bus.
    URL format: /get/sel/ector/%1234?domain=default&download=1
    The forward slash after "/get" is part of the selector
    The selector hash (ex. %1234...) may be replaced with a version (ex. ~-1)
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
        if selector.startswith('/matrix/') and not download:
            # matrix: ([uuids], [labels], np.array, threshold)
            # output: [(uuid, label, [(value, color)])]

            # Sort uuids, labels and indexes by label
            output = list()
            uuids = data[0]
            labels = data[1]
            indexes = range(len(uuids))
            values = data[2]
            valuesflat = sorted(data[2].flat)  # 1-D iterator
            labels, uuids, indexes = zip(*sorted(zip(labels, uuids, indexes)))

            colorclasses = ['info', 'success', 'warning', 'danger']
            nbcolors = len(colorclasses)
            if len(valuesflat) > 0:
                colorthresh = [valuesflat[((i+1)*len(valuesflat))/nbcolors]
                               for i in range((nbcolors-1))]
            else:
                colorthresh = [0] * (nbcolors-1)
            for h1 in range(len(uuids)):
                linecontents = list()
                for h2 in range(len(uuids)):
                    value = values[h1][h2]
                    linecontents.append((value, self.color(colorthresh,
                                                           colorclasses,
                                                           value)))
                output.append((uuids[h1], labels[h1], linecontents))

            self.finish(self.render_string('descriptor/matrix_view',
                                           matrix=output))
        else:
            if type(data) not in [unicode, str]:
                data = str(data)
            self.finish(data)

    def color(self, colorthresh, colorclasses, value):
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


class AgentsHandler(tornado.web.RequestHandler):
    """
    Displays information about agents.
    """
    def get(self):
        self.render('agents.html')

    def post(self, *args, **kwargs):
        # TODO fetch agents descriptions
        domain = self.get_argument('domain', 'default')
        stats, total = self.application.dstore.processed_stats(domain)
        sorted_stats = sorted(stats, key=lambda x: x[1])
        self.finish(dict(agents_stats=sorted_stats, total=total))
