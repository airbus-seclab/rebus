import sys
import logging

log = logging.getLogger("rebus.importer")

def importer_for(path, prefix):
    def import_all(path=path, stop_on_error=False):
        import os,pkgutil
        folder = os.path.dirname(path)
        module = sys.modules[prefix]
        for importer,name,_ in pkgutil.iter_modules([folder]):
            absname = prefix+"."+name
            if absname in sys.modules:
                continue
            loader = importer.find_module(absname)
            try:
                submod = loader.load_module(absname)
            except ImportError,e:
                if stop_on_error:
                    raise
                log.warning("Cannot load REbus plugin [%s]. Root cause: %s" % (name,e))
            else:
                setattr(module, name, submod)
    return import_all
