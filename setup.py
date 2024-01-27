from setuptools import setup, find_packages
import os
import codecs
from data_harvest_reader import __version__
here = os.path.abspath(os.path.dirname(__file__))

with codecs.open(os.path.join(here, "README.md"), encoding="utf-8") as fh:
    LONG_DESCRIPTION = "\n" + fh.read()

VERSION = __version__
DESCRIPTION = 'A class to handle and process multiple files with identical structures within a directory.'

setup(
    name="data-harvest-reader",
    version=VERSION,
    author="Jeferson-Peter (Jeferson Peter)",
    author_email="jeferson.peter@pm.me",
    description=DESCRIPTION,
    url='https://github.com/Jeferson-Peter/data-harvest-reader',
    long_description_content_type="text/markdown",
    long_description=LONG_DESCRIPTION,
    packages=find_packages(),
    install_requires=['polars', 'loguru', 'colorama'],
    keywords=['Python', 'File Reading', 'Multiple File Handler',],
    classifiers=[
        "Development Status :: 1 - Planning",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Operating System :: Unix",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: Microsoft :: Windows",
    ]
)