#!/usr/bin/env python

import os
from distutils.core import setup


setup(name='pydanticrud',
    version='0.1.0',
    description='',
    author='Tim Farrell',
    author_email='tim.farrell@rackspace.com',
    license='MIT',
    url='https://github.com/RSS-Engineering/pydanticrud',
    packages=['pydanticrud'],
    install_requires=[
        "rule-engine==3.2.0",
        "pydantic==1.8.2",
        "boto3==1.17.112",
        "dataclasses==0.8"
    ]
 )
