
def import_all():
    import os,pkgutil
    folder = os.path.dirname(__file__)
    for importer,name,_ in pkgutil.iter_modules([folder]):
        loader = importer.find_module(name)
        loader.load_module(name)

