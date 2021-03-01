#!/usr/bin/env python

from setuptools import setup

setup(name='tap-crossbeam',
      version='0.0.1',
      description='Singer.io tap for extracting data from the Crossbeam API',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_crossbeam'],
      install_requires=[
          'backoff==1.8.0',
          'ratelimit==2.2.1',
          'requests==2.25.1',
          'singer-python==5.10.0'
      ],
      entry_points='''
          [console_scripts]
          tap-crossbeam=tap_crossbeam:main
      ''',
      packages=['tap_crossbeam'],
      package_data = {
          'tap_crossbeam': ['schemas/*.json'],
      }
)
