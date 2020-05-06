#!/usr/bin/env python

from setuptools import find_packages, setup

setup(name='ez_arch_worker',
      version='0.0.1',
      description='Worker API for the ex_arch framework',
      author='Liam Tengelis',
      author_email='liam@tengelisconsulting.com',
      packages=find_packages(),
      package_data={
          '': ['*.yaml'],
      },
)
