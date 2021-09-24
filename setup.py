#!/usr/bin/env python

from setuptools import setup

setup(name='tap-crossbeam',
      version='0.4.1',
      description='Singer.io tap for extracting data from the Crossbeam API',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_crossbeam'],
      install_requires=[
          'backoff==1.8.0', # needs to be pinned because of singer dependencies
          'ratelimit>2',
          'requests>2',
          'singer-python>5'
      ],
      extras_require={
          'test': [
              'pylint',
              'nose',
          ],
          'dev': [
              'ipdb',
          ]
      },
      entry_points='''
          [console_scripts]
          tap-crossbeam=tap_crossbeam:main
      ''',
      packages=['tap_crossbeam'],
      package_data = {
          'tap_crossbeam': ['schemas/*.json'],
      }
)
