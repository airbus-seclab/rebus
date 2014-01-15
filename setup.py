#! /usr/bin/env python

from setuptools import setup

setup(
    name = 'rebus',
    version = '0.1',
    packages=[ 'rebus' ],
    scripts = [ 'bin/rebus_inject' ],

    # Metadata
    author = 'Philippe Biondi',
    author_email = 'phil@secdev.org',
    description = 'REbus: Reverse Engineering Bus',
)
