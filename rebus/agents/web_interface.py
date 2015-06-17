import os
import os.path
from rebus.agent import Agent
import threading
import tornado.ioloop
import tornado.web
import tornado.template
import rebus.agents.inject
from rebus.descriptor import Descriptor
import re
import json


@Agent.register
class WebInterface(Agent):
    _name_ = "web_interface"
    _desc_ = "Display all descriptors exchanged on the bus in a web interface"

    def init_agent(self):
        self.dstore = DescriptorStore(self)
        self.gui = Application(self.dstore)
        self.gui.listen(8080)
        self.ioloop = tornado.ioloop.IOLoop.instance()
        t = threading.Thread(target=self.ioloop.start)
        t.daemon = True
        t.start()

    def process(self, descriptor, sender_id):
        # tornado version must be >= 3.0
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


class CustomTemplate(tornado.template.Template):
    """
    Keeps a dict of functions to be passed to the template at generation time.
    Useful for preprocessing/formatting data.

    For 'analysis' and 'monitor' actions, the RequestHandler is expected to
    pass a 'descrinfos' variable to the template, containing a dictionnary.

    For 'view' actions, the RequestHandler is expected to pass a descriptor
    variable that contains the raw descriptor.
    """
    def __init__(self, template_string, **kwargs):
        if 'functions' in kwargs:
            self._functions = kwargs['functions']
            del kwargs['functions']
        else:
            self._functions = dict()
        super(CustomTemplate, self).__init__(template_string, **kwargs)

    def generate(self, **kwargs):
        kwargs.update(self._functions)
        return super(CustomTemplate, self).generate(**kwargs)


class TemplateLoader(tornado.template.Loader):
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

    def __init__(self, root_directory=None, **kwargs):
        """
        Register existing static files
        """
        if not root_directory:
            root_directory = os.path.join(os.path.dirname(__file__),
                                          'templates')
        super(TemplateLoader, self).__init__(root_directory, **kwargs)

        for fname in os.listdir(os.path.join(root_directory, "descriptor")):
            fullpath = os.path.join(root_directory, "descriptor", fname)
            if not (fname.endswith('.html') and os.path.isfile(fullpath)):
                continue
            #: fname format: desctype_page.html
            selector_prefix, page = fname.rsplit('.', 1)[0].rsplit('_', 1)
            templatestr = open(fullpath, 'rb').read()
            functions = dict()
            TemplateLoader.register(selector_prefix, page, templatestr,
                                    functions)

    @staticmethod
    def register(selector_prefix, page, templatestr, functions):
        """
        Called to register a renderering template for the given page and
        descriptor type.
        """
        TemplateLoader.templates[(selector_prefix, page)] = (templatestr,
                                                             functions)

    @staticmethod
    def register_formatted(template):
        """
        Helper for registering templates and one associated formatter function.

        Syntactic sugar, to be used as a decorator for formatter function.

        Sample use:
        @TemplateLoader.register_formatted(template='selector_prefix_page.html')
        def formatter(...any args, called from template...):

        where 'selector_prefix_page.html' is present under the
        formatted_templates/ directory under module where this decorator is
        being used.

        This template will be used on specified page, for selectors beginning
        with /selector/prefix, unless another registered template has a longer
        selector prefix (e.g. selector_prefix_very_specific_page.html)
        """
        def func_wrapper(f):
            fpath = os.path.dirname(f.__globals__['__file__'])
            templatefile = os.path.join(fpath, 'formatted_templates', template)
            templatestr = open(templatefile, 'rb').read()
            selector_prefix, page = template.rsplit('.', 1)[0].rsplit('_', 1)
            funcdict = {f.__name__: f}
            TemplateLoader.register(selector_prefix, page, templatestr,
                                    funcdict)
            return f
        return func_wrapper

    def resolve_path(self, name, parent_path=None):
        name = super(TemplateLoader, self).resolve_path(name, parent_path)
        return name

    def _create_template(self, name):
        """
        Return the requested template object.
        """

        if not name.startswith('descriptor/'):
            return super(TemplateLoader, self)._create_template(name)
        # '/' (part of selector) are encoded as '_' in template file names.
        # ('_' is forbidden in selectors)
        selector, page = name[11:].replace('/', '_').rsplit('_', 1)

        args = dict()
        args['loader'] = self
        # remove 'descriptor/' from template name
        # iterate to find template with longest selector prefix
        desc_prefix = ""
        for (d, p) in TemplateLoader.templates:
            if page != p:
                continue
            if selector.startswith(d) and len(d) > len(desc_prefix):
                desc_prefix = d
        if desc_prefix != "":
            # load most specific template if exists
            templatestr, funcs = TemplateLoader.templates[(desc_prefix, page)]
        else:
            # use default otherwise
            templatestr, funcs = TemplateLoader.templates[('default', page)]
        args['functions'] = funcs

        template = CustomTemplate(templatestr, **args)
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
            (r"/get([^\?]*)\??.*", DescriptorGetHandler),
            (r"/agents", AgentsHandler),
            (r"/processing/list_processors", ProcessingListHandler),
            (r"/processing/request", ProcessingRequestsHandler),
        ]
        params = {
            'static_path': os.path.join(os.path.dirname(__file__), 'static'),
            'template_loader': TemplateLoader()
        }
        self.dstore = dstore
        self.agent = self.dstore.agent
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
        uuid_label = self.application.agent.list_uuids(domain)
        self.render('uuid.html', domain=domain,
                    selectors=sorted(uuid_label.items(), key=lambda x: x[1]))


class SelectorsHandler(tornado.web.RequestHandler):
    def get(self):
        sels = self.application.agent.find(
            self.get_argument('domain', 'default'), '/.*', limit=100)
        self.render('selectors.html', selectors=sorted(sels))


class MonitorHandler(tornado.web.RequestHandler):
    def get(self):
        self.render('monitor.html')


class DescriptorUpdatesHandler(tornado.web.RequestHandler):
    """
    Dispatches descriptors to web clients.
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
                    d['html_' + page] = \
                        self.render_string('descriptor%s_%s' % (d['selector'],
                                                                page),
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
        desc = self.application.agent.get_descriptor(domain, selector)
        if desc is None:
            self.send_error(status_code=404)
            return

        if download:
            self.set_header('Content-Disposition', 'attachment; filename=%s' %
                            tornado.escape.url_escape(desc.label))
            self.finish(desc.value)

        else:
            self.render('descriptor%s_view' % desc.selector, descriptor=desc)


class InjectHandler(tornado.web.RequestHandler):
    """
    Injects a file to the bus.
    """
    def post(self, *args, **kwargs):
        f = self.request.files['file'][0]
        uuid = self.application.agent.inject(f['filename'], f['body'])
        self.finish(dict(uuid=uuid))


class ProcessingListHandler(tornado.web.RequestHandler):
    """
    Lists (agents, config) that could process this descriptor
    """
    def post(self, *args, **kwargs):
        domain = self.get_argument('domain')
        selector = self.get_argument('selector')
        agents = self.application.agent.get_processable(str(domain),
                                                        str(selector))
        agents = [(name, ', '.join(
            ["%s=%s" % (k, v) for (k, v) in json.loads(config_txt).items()]
                )) for (name, config_txt) in agents]
        self.finish(self.render_string('request_processing_popover.html',
                                       agents=agents))


class ProcessingRequestsHandler(tornado.web.RequestHandler):
    """
    Requests processing of this descriptor by listed agents
    """
    def post(self, *args, **kwargs):
        params = json.loads(self.request.body)
        if not all([i in params for i in ('domain', 'selector', 'targets')]):
            self.send_error(400)
        if not all([isinstance(i, unicode) for i in params['targets']]):
            self.send_error(400)
        self.application.agent.request_processing(str(params['domain']),
                                                  str(params['selector']),
                                                  list(params['targets']))
        self.finish()


class AgentsHandler(tornado.web.RequestHandler):
    """
    Displays information about agents.
    """
    def get(self):
        self.render('agents.html')

    def post(self, *args, **kwargs):
        # TODO fetch agents descriptions
        domain = self.get_argument('domain', 'default')
        processed, total = self.application.agent.processed_stats(domain)
        agent_count = {k: [k, v, 0] for k, v in
                       self.application.agent.list_agents().items()}
        for agent, nbprocessed in processed:
            if agent in agent_count:
                # agent is still registered
                agent_count[agent][2] = nbprocessed

        stats = list()
        for agent in sorted(agent_count):
            stats.append(agent_count[agent])
        self.finish(dict(agents_stats=stats, total=total))
