import sys
import logging

log = logging.getLogger("rebus.importer")

def importer_for(path, prefix):
    def import_all(path=path, stop_on_error=False):
        import os,pkgutil
        folder = os.path.dirname(path)
        for importer,name,_ in pkgutil.iter_modules([folder]):
            name = prefix+"."+name
            if name in sys.modules:
                continue
            loader = importer.find_module(name)
            try:
                loader.load_module(name)
            except ImportError,e:
                if stop_on_error:
                    raise
                log.warning("Cannot load REbus plugin [%s]. Root cause: %s" % (name,e))
    return import_all
