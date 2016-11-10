"""
Helpers to pick a selector based on its contents.
Typically used before injecting data to the bus, when the selector has not
already been determined by user-input or by the agent..

To use this module to pick a selector, import the following module:
from rebus.tools.selector import guess_selector
Then call guess_selector(fname, buf, label)

To register a new function that can guess selectors, use the following
construct:

from rebus.tools import selectors

@selectors.register
def my_function(fname, buf, label, magic_txt, default_selector):
    if is_foobar(value):
       return 10, "/foobar"
"""
import struct
from rebus.tools import magic_wrap


_guess_functions = []


def register(f):
    """
    The expected return value for such registered guessing function is either:
    * None, meaning no type could be guessed by this function
    * (score: int, selector: str) having score > 0. If several guessing
      functions return a selector, then one of the selectors having the highest
      confidence score is chosen.

    Expected signature for registered functions:
    :param magic_txt: string returned by the "magic" module
    :param label: descriptor's label
    :param default_selector: selector that has been determined by rebus'
        default method, _default_guess
    :param value: descriptor's value
    """
    _guess_functions.append(f)
    return f


def guess_selector(fname=None, buf=None, label=None):
    """
    Called by agents that want to pick a selector.

    :param fname: local file name
    :param buf: descriptor contents as str
    :param label: descriptor label
    """
    if fname is not None:
        magic_txt = magic_wrap.from_file(fname)
    elif buf is not None:
        magic_txt = magic_wrap.from_buffer(buf)
    else:
        raise Exception("Either fname or buffer must be set when calling "
                        "guess_selector.")
    default_selector = selector = _default_guess(fname, buf, label, magic_txt)
    score = 0
    for guess_fun in _guess_functions:
        res = guess_fun(fname, buf, label, magic_txt, default_selector)
        if res:
            new_score, new_selector = res
            if new_score > score:
                selector = new_selector
                score = new_score
    return selector


def _default_guess(fname=None, buf=None, label=None, magic_txt=""):
    """
    Contains common selector determination functions. Agents that want to pick
    a selector should call guess_selector instead.

    """
    # Executable files
    if ".Net" in magic_txt:
        return "/binary/net"
    if "ELF" in magic_txt and ("sharedobject" in magic_txt or
                               "executable" in magic_txt):
        return "/binary/elf"
    if "executable" in magic_txt and "PE32" in magic_txt:
        return "/binary/pe"
    if "DOS" in magic_txt:
        # libmagic is a bit buggy
        # make sure it's not a PE
        try:
            if fname is not None:
                buf = open(fname).read()
            if buf is not None:
                # MZ.e_lfanew
                e_lfanew = struct.unpack('<I', buf[0x3C:0x3C+4])[0]
                if buf[e_lfanew:e_lfanew+4] == "PE\x00\x00":
                    return "/binary/pe"
        except:
            return "/binary/dos"
        return "/binary/dos"
    if "Mach-O" in magic_txt:
        return "/binary/macho"

    # Compressed files
    if "gzip compressed data" in magic_txt:
        return "/compressed/gzip"
    if "bzip2 compressed data" in magic_txt:
        return "/compressed/bzip2"

    # Archive files
    if "POSIX tar archive" in magic_txt:
        return "/archive/tar"
    if "Zip archive data" in magic_txt:
        return "/archive/zip"
    if "Microsoft Cabinet archive data" in magic_txt:
        return "/archive/cab"
    if "Java Jar file data" in magic_txt:
        return "/archive/jar"
    if "RAR archive data" in magic_txt:
        return "/archive/rar"

    # E-mails
    if 'Composite Document File V2 Document' in magic_txt:
        if label and label.endswith('.msg'):
            return "/email/msg"
    if 'ASCII text' in magic_txt:
        if label and label.endswith('.eml'):
            return '/email/eml'
        # TODO peek at contents & grep headers to identify e-mail?
    if 'RFC 822 mail, ASCII text' in magic_txt:
        return '/email/eml'
    if 'SMTP mail, ASCII text' in magic_txt:
        return '/email/eml'

    # Documents
    if 'PDF document, version' in magic_txt:
        return "/document/pdf"
    if 'Rich Text Format' in magic_txt:
        return "/document/rtf"
    if 'Microsoft Word 2007+' in magic_txt:
        return "/document/msoffice/docx"
    if 'Microsoft Excel 2007+' in magic_txt:
        return "/document/msoffice/xlsx"
    if 'Microsoft PowerPoint 2007+' in magic_txt:
        return "/document/msoffice/pptx"
    if 'Composite Document File V2 Document' in magic_txt:
        if 'MSI Installer' in magic_txt:
            return "/binary/msi"
        if 'Microsoft Excel' in magic_txt:
            return "/document/msoffice/xls"
        if 'Microsoft PowerPoint' in magic_txt:
            return "/document/msoffice/ppt"
        if label:
            if label.endswith('.ppt'):
                return "/document/msoffice/ppt"
            if label.endswith('.pptx'):
                return "/document/msoffice/pptx"
            if label.endswith('.xls'):
                return "/document/msoffice/xls"
            if label.endswith('.xlsx'):
                return "/document/msoffice/xlsx"
            if label.endswith('.doc'):
                return "/document/msoffice/doc"
            if label.endswith('.docx'):
                return "/document/msoffice/docx"
        return "/document/doc"

    # ASCII text
    if 'ASCII text' in magic_txt:
        return "/text/ascii"

    return "/unknown"
