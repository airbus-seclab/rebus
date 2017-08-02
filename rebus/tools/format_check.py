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


def processing_depth(store, descriptor):
    """
    * Avoid loops (d1.precursors=[d2.selector] AND d2.precursors=[d1.selector])
    * Forbid having >3 precursors at different depths (ex. a precursor (parent),
        and a precursor of that precursor (~grandparent)) having the same
        selector (excluding hash) - used to ensure analyses terminate
    """
    selector_prefix = descriptor.selector.split('%')[0]
    levelset = set()
    to_review = [(0, sel) for sel in descriptor.precursors]
    while to_review:
        l, s = to_review.pop()
        if l > 1000:
            # avoid loops
            return False
        prefix = s.split('%')[0]
        if prefix == selector_prefix:
            levelset.add(l)
        d = store.get_descriptor(descriptor.domain, s)
        if not d:
            # precursor does not exist: refuse this.
            return False
        to_review.extend([(l+1, sel) for sel in d.precursors])
        if len(levelset) > 2:
            return False
    return True
