import magic

# Determine which flavour of libmagic binding is available:
# * native bindings from http://darwinsys.com/file/
# * bindings from https://pypi.python.org/pypi/python-magic

ms = None
if 'open' in dir(magic):
    # native bindings
    native = True
    ms = magic.open(magic.MAGIC_NONE)
    ms.load()
else:
    native = False


def from_file(fname):
    if native:
        return ms.file(fname)
    else:
        return magic.from_file(fname)


def from_buffer(data):
    if native:
        return ms.buffer(data)
    else:
        return magic.from_buffer(data)
