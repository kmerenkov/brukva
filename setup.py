#!/usr/bin/env python

try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup

VERSION = '0.0.1'

setup(name='brukva',
      version=VERSION,
      description='Asynchronous Redis client that works within the Tornado IO loop',
      author='Konstantin Merenkov',
      author_email='kmerenkov@gmail.com',
      license='WTFPL',
      url='http://github.com/kmerenkov/brukva',
      keywords=['Redis', 'Tornado'],
      packages= find_packages(),
      py_modules=['brukva'],
      test_suite='tests.all_tests',
    )
