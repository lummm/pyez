#!/usr/bin/env python

from setuptools import find_packages, setup


setup(name='pyez',
      version='v0.1.1',
      description='Worker API for the EZ Arch framework',
      author='Liam Tengelis',
      author_email='liam@tengelisconsulting.com',
      url='https://github.com/tengelisconsulting/pyez',
      download_url=("https://github.com"
                    "/tengelisconsulting/pyez"
                    "/archive/v0.1.1.tar.gz"),
      packages=find_packages(),
      package_data={
          '': ['*.yaml'],
      },
      python_requires='>=3.8')
