from __future__ import absolute_import, division, print_function

import io
import re
from os import path

from setuptools import find_packages, setup

with io.open("pennsieve/__init__.py", "r") as fd:
    version = re.search(
        r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]', fd.read(), re.MULTILINE
    ).group(1)

if not version:
    raise RuntimeError("Cannot find version information")

here = path.abspath(path.dirname(__file__))

with io.open(path.join(here, "requirements.txt"), mode="r", encoding="utf-8") as f:
    reqs = [line.strip() for line in f if not line.startswith("#")]

# Get the long description from the README file
with io.open(path.join(here, "README.rst"), encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="pennsieve",
    version=version,
    author="Pennsieve, Inc.",
    author_email="joost@pennsieve.com",
    description="Python client for the Pennsieve Platform",
    long_description=long_description,
    packages=find_packages(),
    package_dir={"pennsieve": "pennsieve"},
    setup_requires=["cython"],
    install_requires=reqs,
    extras_require={
        "data": ["numpy>=1.13", "pandas>=0.20"],
    },
    python_requires=">=2.7, !=3.0.*, !=3.1.*, !=3.2.*, !=3.3.*, <4.0",
    entry_points={
        "console_scripts": ["pennsieve-profile=pennsieve.cli.pennsieve_profile:main"]
    },
    license="",
    keywords="pennsieve client rest api",
    url="https://github.com/Pennsieve/pennsieve-python",
    project_urls={
        "Pennsieve": "https://www.pennsieve.com",
        # Do not remove or rename this "Documentation" URL: it is used by
        # the Sphinx theme to generate links to previous doc versions.
        # However, you can change the URL with no issues.
        "Documentation": "https://developer.pennsieve.io/python",
        "Bug Reports": "https://github.com/Pennsieve/pennsieve-python/issues",
    },
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Topic :: Utilities",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
)
