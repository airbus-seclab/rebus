#! /usr/bin/env python

from setuptools import setup
from glob import glob

setup(
    name='rebus',
    version='0.4',
    packages=[
        'rebus', 'rebus/buses', 'rebus/buses/dbusbus', 'rebus/buses/rabbitbus',
        'rebus/agents', 'rebus/tools', 'rebus/storage_backends'],
    classifiers=[
        'License :: OSI Approved :: BSD License'
    ],
    package_data={'rebus/agents': [
        'static/*.js',
        'static/*.css',
        'static/bootstrap-3.1.1-dist/css/*.css',
        'static/bootstrap-3.1.1-dist/css/*.map',
        'static/bootstrap-3.1.1-dist/fonts/*.eot',
        'static/bootstrap-3.1.1-dist/fonts/*.svg',
        'static/bootstrap-3.1.1-dist/fonts/*.ttf',
        'static/bootstrap-3.1.1-dist/fonts/*.woff',
        'static/bootstrap-3.1.1-dist/js/*.js',
        'static/jquery-file-upload/*.js',
        'templates/*.html',
        'templates/descriptor/*.html']},
    scripts=[
        'bin/rebus_master_dbus', 'bin/rebus_master_rabbit', 'bin/rebus_agent',
        'bin/rebus_infra', 'bin/rebus_master'],
    install_requires=[
        'pika',
        'larch-pickle'
    ],
    data_files=[
        ('etc/rebus', ['conf/dbus_session.conf']),
        ('etc/rebus/services', glob('conf/services/*.service')),
        ('etc/rebus', ['conf/rebus-infra-config-example.yaml']),
    ],

    # Metadata
    author='Philippe Biondi',
    author_email='phil@secdev.org',
    description='REbus: Reverse Engineering Bus',
)
