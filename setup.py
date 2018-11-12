#!/usr/bin/env python

import os
import sys

from setuptools import setup

version = "0.1.0"

if sys.argv[-1] == 'publish':
    os.system('python setup.py sdist upload')
    os.system('python setup.py bdist_wheel upload')
    sys.exit()

if sys.argv[-1] == 'tag':
    os.system("git tag -a %s -m 'version %s'" % (version, version))
    os.system("git push --tags")
    sys.exit()

with open('README.md') as readme_file:
    readme = readme_file.read()

with open('HISTORY.md') as history_file:
    history = history_file.read().replace('.. :changelog:', '')

requirements = [
    'pathos',
    'pyyaml',
    'toolz',
    'kafka',
    'prometheus_client',
    'avro',
    'fastavro',
    'confluent-kafka'
]

# http://blog.prabeeshk.com/blog/2014/10/31/install-apache-spark-on-ubuntu-14-dot-04/

long_description = readme + '\n\n' + history

if sys.argv[-1] == 'readme':
    print(long_description)
    sys.exit()


ENTRY_POINTS = {
}

setup(
    name='spp',
    version=version,
    description=('Python library for processing of seismic data'),
    long_description=long_description,
    author='spp development team',
    author_email='',
    url='',
    packages=[
        'spp',
    ],
    package_dir={'spp': 'spp'},
    entry_points=ENTRY_POINTS,
    include_package_data=True,
    install_requires=requirements,
    license='GNU Lesser General Public License, Version 3 (LGPLv3)',
    zip_safe=False,
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Intended Audience :: Science/Research',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: OSI Approved :: GNU General Public License v3',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy',
        'Topic :: Scientific/Engineering',
    ],
    keywords=(
        'microquake, seismology, mining, microseismic, signal processing, '
        'event location, 3D velocity, automatic, processing, Python, '
        'focal mechanism'
    ),
)





