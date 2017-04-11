"""
Helper functions to check format of various REbus structures
* selector strings
* full selector strings
* domain strings
"""
import re

#: selector, optionally followed by hash or version number
_ALLOWED_SELECTOR_REGEX = re.compile(
    r'^/[a-zA-Z0-9/_-]+(|%[a-f0-9]{64}|~-?\d+)$')
#: selector, followed by hash
_ALLOWED_FULLSELECTOR_REGEX = re.compile(
    r'/[a-zA-Z0-9/_-]+%[a-f0-9]{64}')
_ALLOWED_DOMAIN_REGEX = re.compile(r'^[a-zA-Z0-9-]*$')


def is_valid_domain(domain):
    """
    Checks a domain string
    """
    return _ALLOWED_DOMAIN_REGEX.match(domain)


def is_valid_selector(selector):
    """
    Checks a selector string, which may include a hash.
    Example valid selectors:
    /sel/ector
    /sel/ector/
    /sel/ector%e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
    /sel/ector/%e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
    /sel/ector/~-1
    """
    return _ALLOWED_SELECTOR_REGEX.match(selector)


def is_valid_fullselector(fullselector):
    """
    Checks selector string
    """
    return _ALLOWED_FULLSELECTOR_REGEX.match(fullselector)
