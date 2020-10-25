#!/usr/bin/env python

from setuptools import find_packages, setup


setup(name='ezpy',
      version='v0.1.0',
      description='Worker API for the EZ Arch framework',
      author='Liam Tengelis',
      author_email='liam@tengelisconsulting.com',
      url='https://github.com/tengelisconsulting/ezpy',
      download_url=("https://github.com"
                    "/tengelisconsulting/ezpy"
                    "/archive/v0.1.0.tar.gz"),
      packages=find_packages(),
      package_data={
          '': ['*.yaml'],
      },
)
