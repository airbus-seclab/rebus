import os
import os.path
from rebus.agent import Agent
import threading
import datetime
import time
import tornado.autoreload
import tornado.ioloop
import tornado.web
import tornado.template
import rebus.agents.inject
from rebus.descriptor import Descriptor
import re
import json


class AsyncProxy(object):
    """
    Provides methods for making API requests from the main thread.

    When DBus is used, DBus method calls must not be called from the tornado
    thread; symptoms include expired 25s DBus timeouts, during which the web
    server freezes.
    """
    def __init__(self, agent):
        self._agent = agent

    def __getattr__(self, attr):
        if not attr.startswith('async_'):
            raise AttributeError
        method_name = attr[6:]

        if hasattr(self, method_name + '_buscallback'):
            method = None
            bus_callback = getattr(self, method_name + '_buscallback')
        else:
            method = getattr(self._agent.bus, method_name)

            def bus_callback(method, callback, *args):
                results = method(self._agent, *args)
                self._agent.ioloop.add_callback(callback, results)
                # dbus-specific - indicates this method should only be called
                # once
                return False

        def _async(callback, *args):
            self._agent.bus.busthread_call(bus_callback, method, callback,
                                           *args)
        return _async

    def getwithvalue_buscallback(self, method, callback, *args):
        """
        Ensures descriptor's values are retrieved before passing a descriptor
        to the web server thread, to avoid DBus calls when the value @property
        is read.
        """
        desc = self._agent.bus.get(self._agent, *args)
        # force value retrieval
        value = desc.value
        self._agent.ioloop.add_callback(callback, desc)
        return False

    def find_by_uuid_withvalue_buscallback(self, method, callback, *args):
        """
        Ensures descriptor's values are retrieved before passing a descriptor
        to the web server thread, to avoid DBus calls when the value @property
        is read.
        """
        descs = self._agent.bus.find_by_uuid(self._agent, *args)
        # force value retrieval
        for desc in descs:
            value = desc.value
        self._agent.ioloop.add_callback(callback, descs)
        return False


@Agent.register
class WebInterface(Agent):
    _name_ = "web_interface"
    _desc_ = "Display all descriptors exchanged on the bus in a web interface"

    @classmethod
    def add_arguments(cls, subparser):
        subparser.add_argument(
            '--autoreload', action='store_true',
            help='Auto reload static files - use for development')

    def init_agent(self):
        # Build list of async methods, to be used from the tornado thread
        self.async_proxy = AsyncProxy(self)

        self.dstore = DescriptorStore(self, self.async_proxy)
        self.ioloop = tornado.ioloop.IOLoop.instance()
        self.gui = Application(self.dstore, self.async_proxy, self.ioloop,
                               autoreload=self.config['autoreload'])
        self.gui.listen(8080)
        t = threading.Thread(target=self.ioloop.start)
        t.daemon = True
        t.start()

    def process(self, descriptor, sender_id):
        # tornado version must be >= 3.0
        # force value retrieval
        value = descriptor.value
        self.ioloop.add_callback(self.dstore.new_descriptor, descriptor,
                                 sender_id)


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
            self.register_file_descriptor_template(fullpath, fname)

    def register_file_descriptor_template(self, fullpath, fname):
        if not (fname.endswith('.html') and os.path.isfile(fullpath)):
            return
        #: fname format: desctype_page.html
        try:
            selector_prefix, page = fname.rsplit('.', 1)[0].rsplit('_', 1)
        except ValueError:
            raise ValueError("Invalid descriptor template name %s" %
                             fullpath)

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
    def __init__(self, dstore, async, ioloop, autoreload):
        handlers = [
            (r"/", tornado.web.RedirectHandler, {'url': '/monitor'}),
            (r"/monitor", MonitorHandler),
            (r"/inject", InjectHandler),
            (r"/uuid/(.*)", AnalysisListHandler),
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
        if autoreload:
            params['autoreload'] = True
            for path in ('templates', 'static'):
                for d, _, files in os.walk(os.path.dirname(__file__), path):
                    for f in files:
                        tornado.autoreload.watch(os.path.join(d, f))

        self.dstore = dstore
        self.async = async
        self.ioloop = ioloop
        tornado.web.Application.__init__(self, handlers, **params)


class DescriptorStore(object):
    def __init__(self, agent, async):

        self.async = async
        #: self.waiters is a set of (domain, uuid, callback) for new
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
            if domain and uuid:
                self.async.async_find_by_uuid_withvalue(callback, domain, uuid)
                return

        else:
            # Return cached descriptors that are newer than cursor (all cached
            # if cursor is not in cache anymore).
            # Also works for cursor == 'cached'
            new_count = 0
            for desc in reversed(self.cache):
                if desc.hash == cursor:
                    break
                new_count += 1
            if new_count > 0:
                # New descriptors are available. Send them if they match.
                for desc in self.cache[-new_count:]:
                    if domain == desc.domain or not domain:
                        if uuid == desc.uuid or not uuid:
                            matching_infos.append(desc)
                if matching_infos:
                    callback(matching_infos)
                    return

        if matching_infos:
            callback(matching_infos)
            return
        # No new matching descriptors have been found, start waiting
        self.add_to_waitlist(domain, uuid, callback)

    def add_to_waitlist(self, domain, uuid, callback):
        """
        :param callback: method of a RequestHandler instance
        """
        self.waiters.add((domain, uuid, callback))

    def info_from_descs(self, descriptors):
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
                'precursors': desc.precursors,
                'version': desc.version,
            }
            if desc.selector.startswith('/link/'):
                descrinfo['value'] = desc.value
                descrinfo['linksrchash'] = desc.value['selector'].split('%')[1]
            descrinfos.append(descrinfo)
        return descrinfos

    def cancel_wait(self, callback):
        for (domain, uuid, cb) in set(self.waiters):
            if callback == cb:
                self.waiters.remove((domain, uuid, cb))

    def new_descriptor(self, descriptor, sender_id):
        """
        :param descriptor: new descriptor
        :param sender_id: sender agent ID

        Callback function
        Called whenever a new descriptor is available (received from bus, or
        injected by web_interface)
        """
        for (domain, uuid, callback) in list(self.waiters):
            if domain == descriptor.domain or not domain:
                if uuid == descriptor.uuid or not uuid:
                    callback([descriptor])
                    self.waiters.remove((domain, uuid, callback))
        self.cache.append(descriptor)
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


class AnalysisListHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self, domain):
        if domain == '':
            domain = 'default'
        self.domain = domain
        self.application.async.async_list_uuids(self.send_results_cb, domain)

    def send_results_cb(self, uuid_label):
        if self.request.connection.stream.closed():
            return
        self.render('uuid.html', domain=self.domain,
                    selectors=sorted(uuid_label.items(), key=lambda x: x[1]))


class SelectorsHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self):
        self.application.async.async_find(
            self.get_selectors_cb,
            self.get_argument('domain', 'default'), '/.*', 100)

    def get_selectors_cb(self, sels):
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
        self.cursor = self.get_argument('cursor')
        self.page = self.get_argument('page')
        self.domain = self.get_argument('domain')
        self.uuid = self.get_argument('uuid')
        self.application.dstore.wait_for_descriptors(self.on_new_descriptors,
                                                     self.domain, self.uuid,
                                                     self.page, self.cursor)

    def on_new_descriptors(self, descs):
        if self.request.connection.stream.closed():
            return

        if not descs:
            self.application.dstore.add_to_waitlist(self.domain, self.uuid,
                                                    self.on_new_descriptors)
            return
        descrinfos = self.application.dstore.info_from_descs(descs)

        #: Contains only data from descrinfos needed to render page
        infos = []
        for d in descrinfos:
            info = {}
            infos.append(info)
            for k in ('hash', 'selector', 'fullselector', 'printablevalue',
                      'agent', 'domain', 'label', 'linksrchash',
                      'version', 'processing_time'):
                if k in d:
                    info[k] = d[k]
            if self.page in ('monitor', 'analysis'):
                d['html_' + self.page] = \
                    self.render_string('descriptor%s_%s' % (d['selector'],
                                                            self.page),
                                       descriptor=d)

                info['html'] = d['html_' + self.page]
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
    @tornado.web.asynchronous
    def get(self, selector='', *args, **kwargs):
        domain = self.get_argument('domain', 'default')
        self.application.async.async_getwithvalue(self.process_get_results,
                                                  domain, selector)

    def process_get_results(self, desc):
        download = (self.get_argument('download', '0') == '1')
        if desc is None:
            self.send_error(status_code=404)
            return
        value = desc.value
        if download:
            self.set_header('Content-Disposition', 'attachment; filename=%s' %
                            tornado.escape.url_escape(desc.label))
            self.finish(str(value))

        else:
            if type(value) is list:
                self.finish(json.dumps(dict(list=value)))
            elif type(value) is dict:
                self.finish(json.dumps(value))
            else:
                self.render('descriptor%s_view' % desc.selector,
                            descriptor=desc)


class InjectHandler(tornado.web.RequestHandler):
    """
    Injects a file to the bus.
    """
    def post(self, *args, **kwargs):
        t1 = time.time()
        self.f = self.request.files['file'][0]
        self.filename = self.f['filename']
        value = self.f['body']
        agentname = 'web_interface_inject'
        selector = rebus.agents.inject.guess_selector(buf=value,
                                                      label=self.filename)
        domain = "default"  # TODO allow user to specify domain
        processing_time = time.time() - t1
        filedesc = Descriptor(self.filename, selector, value, domain,
                              agent=agentname, processing_time=processing_time)
        self.uuid = filedesc.uuid
        self.desc = filedesc
        self.application.async.async_push(self.process_inject_results,
                                          self.desc)
        submission_data = {'filename': self.filename,
                           'date': datetime.datetime.now().isoformat()}
        submdesc = filedesc.spawn_descriptor('/submission/',
                                             submission_data,
                                             agentname)
        self.desc = submdesc
        self.application.async.async_push(self.process_inject_results,
                                          submdesc)
        self.finish(dict(uuid=self.uuid, filename=self.filename))

    def process_inject_results(self, result):
        self.application.ioloop.add_callback(
            self.application.dstore.new_descriptor,
            self.desc, "storage-0")


class ProcessingListHandler(tornado.web.RequestHandler):
    """
    Lists (agents, config) that could process this descriptor
    """
    @tornado.web.asynchronous
    def post(self, *args, **kwargs):
        domain = self.get_argument('domain')
        selector = self.get_argument('selector')
        self.application.async.async_get_processable(
            self.processing_list_cb, str(domain), str(selector))

    def processing_list_cb(self, agents):
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
        self.application.async.async_request_processing(
            lambda x: None,
            str(params['domain']),
            str(params['selector']),
            list(params['targets']))
        self.finish()


class AgentsHandler(tornado.web.RequestHandler):
    """
    Displays information about agents.
    """
    def get(self):
        self.render('agents.html')

    @tornado.web.asynchronous
    def post(self, *args, **kwargs):
        # TODO fetch agents descriptions
        domain = self.get_argument('domain', 'default')
        self.application.async.async_processed_stats(self.agents_cb1, domain)

    def agents_cb1(self, params):
        self.processed, self.total = params
        self.application.async.async_list_agents(self.agents_cb2)

    def agents_cb2(self, res):
        agent_count = {k: [k, v, 0] for k, v in res.items()}
        for agent, nbprocessed in self.processed:
            if agent in agent_count:
                # agent is still registered
                agent_count[agent][2] = nbprocessed

        stats = list()
        for agent in sorted(agent_count):
            stats.append(agent_count[agent])
        self.finish(dict(agents_stats=stats, total=self.total))
