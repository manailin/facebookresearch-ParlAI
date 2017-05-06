# Copyright (c) 2017-present, Facebook, Inc.
# All rights reserved.
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

from setuptools import setup, find_packages

with open('README.md') as f:
    readme = f.read()

with open('LICENSE') as f:
    license_ = f.read()

with open('requirements.txt') as f:
    reqs = f.read().strip().split()

setup(
    name='parlai',
    version='0.1.0',
    description=('A framework for training and evaluating AI models on a ' +
                 'variety of openly available dialog datasets.'),
    long_description=readme,
    url='http://parl.ai/',
    license=license_,
    packages=find_packages(exclude=(
        'data', 'docs', 'downloads', 'examples', 'logs', 'tests')),
    install_requires=reqs,
    maintainer='Alexander H Miller',
    maintainer_email='ahm@fb.com',
)
