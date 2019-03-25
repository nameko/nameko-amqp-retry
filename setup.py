#!/usr/bin/env python
from setuptools import find_packages, setup

setup(
    name='nameko-amqp-retry',
    version='0.7.1',
    description='Nameko extension allowing AMQP entrypoints to retry later',
    author='Student.com',
    url='http://github.com/nameko/nameko-amqp-retry',
    packages=find_packages(exclude=['test', 'test.*']),
    install_requires=[
        "nameko>=2.7.0",
        "kombu"
    ],
    extras_require={
        'dev': [
            "arrow==0.8.0",
            "coverage==4.0.3",
            "flake8==2.5.0",
            "pylint==1.5.1",
            "pytest==2.8.3",
        ],
        'examples': [
            "arrow==0.8.0"
        ]
    },
    dependency_links=[],
    zip_safe=True,
    license='Apache License, Version 2.0',
    classifiers=[
        "Programming Language :: Python",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.3",
        "Programming Language :: Python :: 3.4",
        "Topic :: Internet",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Intended Audience :: Developers",
    ]
)
