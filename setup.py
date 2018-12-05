# -*- coding: utf-8 -*-

from setuptools import setup

version = '0.0.3'

with open('requirements.txt') as _file:
    requires = [i.strip() for i in _file]

with open('README.rst') as file:
    long_description = file.read()

setup(
    name='lian',
    version=version,
    packages=['lian', 'lian.orm', 'lian.utils'],
    license='http://www.apache.org/licenses/LICENSE-2.0',
    author='Hu Ang',
    author_email='ninedoors@126.com',
    url='https://www.sendcloud.com/',
    description='lian is a python toolkit',
    long_description=long_description,
    install_requires=requires,
    python_requires='>=2.7.9,>=3.4',
    classifiers=[
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy',
    ],
)
