import os
import re
import sys

from setuptools import find_packages, setup

if sys.version_info < (3, 6):
    sys.exit('Sorry, Python < 3.6 is not supported')

with open('VERSION') as fh:
    __version__ = fh.read().strip()

build_version = __version__
if 'BUILD' in os.environ:
    spec = re.sub(r'[^0-9]', '', os.environ['BUILD'])
    build_version = "{}-{}".format(__version__, spec)

here = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(here, 'README.rst')) as f:
    README = f.read()


requires = [
    'python-qpid-proton>=0.18.1',
    'PyYAML>=3.12'
]

test_requires = requires + [
    'astroid<1.6.0',
    'bandit',
    'mypy==0.501',
    'pylint==1.7.0',
    'pytest',
    'pytest-cov',
]

doc_requires = requires + [
    'sphinx',
    'sphinx-rtd-theme',
    'sphinxcontrib-asyncio',
]

setup(
    name='qpid-bow',
    version=build_version,
    packages=find_packages(),
    install_requires=requires,
    tests_require=test_requires,
    extras_require={
        'test': test_requires,
        'docs': doc_requires,
    },
    include_package_data=True,
    zip_safe=True,
    entry_points={
        'console_scripts': ['qb=qpid_bow.cli.qpid_bow:main']
    },
    description='Set of tools to work with Qpid and a library for '
                'high-level Qpid interaction.',
    long_description=README,
    author='Bynder',
    author_email='info@bynder.com',
    url='https://www.bynder.com/',
    license='MIT',
    keywords='bynder',
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Topic :: Software Development',
        'Topic :: Utilities',
    ],
)
