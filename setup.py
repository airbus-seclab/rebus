#! /usr/bin/env python

from setuptools import setup
from glob import glob

setup(
    name = 'rebus',
    version = '0.1',
    packages=[ 'rebus', 'rebus/buses', 'rebus/buses/dbusbus', 'rebus/agents', 'rebus/tools'],
    package_data={'rebus/agents': ['static/*.js',
        'static/*.css',
        'static/bootstrap-3.1.1-dist/css/*',
        'static/bootstrap-3.1.1-dist/fonts/*',
        'static/bootstrap-3.1.1-dist/js/*',
        'static/jquery-file-upload/*',
        'templates/*']},
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
