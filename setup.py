#! /usr/bin/env python

from setuptools import setup
from glob import glob

setup(
    name = 'rebus',
    version = '0.1',
    packages=[ 'rebus', 'rebus/buses', 'rebus/buses/dbusbus', 'rebus/agents', 'rebus/tools'],
    scripts = [ 'bin/rebus_master_dbus', 'bin/rebus_agent' ],
    data_files = [
        ('etc/rebus', ['conf/dbus_session.conf']),
        ('etc/rebus/services', glob('conf/services/*.service')),
    ],

    # Metadata
    author = 'Philippe Biondi',
    author_email = 'phil@secdev.org',
    description = 'REbus: Reverse Engineering Bus',
)
