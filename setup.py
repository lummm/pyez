#!/usr/bin/env python

from setuptools import find_packages, setup

setup(name='ez_arch_worker',
      version='v0.1.1',
      description='Worker API for the ez_arch framework',
      author='Liam Tengelis',
      author_email='liam@tengelisconsulting.com',
      url = 'https://github.com/tengelisconsulting/ez_arch_worker',
      download_url = 'https://github.com/tengelisconsulting/ez_arch_worker/archive/v0.1.1.tar.gz',
      packages=find_packages(),
      package_data={
          '': ['*.yaml'],
      },
)
