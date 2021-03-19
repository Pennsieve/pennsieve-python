pennsieve-python
================

.. image:: https://travis-ci.org/Pennsieve/pennsieve-python.svg?branch=master
    :target: https://travis-ci.org/Pennsieve/pennsieve-python
.. image:: https://codecov.io/gh/Pennsieve/pennsieve-python/branch/master/graph/badge.svg
    :target: https://codecov.io/gh/Pennsieve/pennsieve-python
.. image:: https://img.shields.io/pypi/pyversions/pennsieve.svg
    :target: https://pypi.org/project/pennsieve/

Python client and command line tool for Pennsieve.

Installation
------------

To install, run

.. code:: bash

    pip install -U pennsieve

See the `installation notes`_ for more details.

.. _installation notes: https://github.com/Pennsieve/pennsieve-python/blob/master/INSTALL.rst

Release
-------

To release, you should:

- Change CHANGELOG.md with the proper changes for the release
- Modify the version in __init.py__
- Merge the branch into master
- Create a new Github Release

Documentation
-------------

Client and command line documentation can be found on `Pennsieve’s
documentation website`_.

.. _Pennsieve’s documentation website: http://developer.pennsieve.io/python

You can also `contribute`_ to Pennsieve's documentation to improve this project and help others learn.

.. _contribute: https://github.com/Pennsieve/pennsieve-python/blob/master/docs/CONTRIBUTION_TEMPLATE.md

Tests
-------------
Install the test requirements before running `pytest`_:

.. _pytest: https://docs.pytest.org/en/latest/usage.html

.. code:: bash

    make install
    pytest

To run the Pennsieve CLI Agent integration tests, you need to `install the agent`_
and run the tests with the `--agent` argument:

.. _install the agent: https://developer.pennsieve.io/agent/index.html

.. code:: bash

    pytest --agent


Contribution
-------------

Please make sure to read the `Contributing Guide`_ before making a pull request.

.. _Contributing Guide: https://github.com/Pennsieve/pennsieve-python/blob/master/docs/CONTRIBUTION_TEMPLATE.md



Changelog
-------------

Changes for each release are documented in the `release notes`_.

.. _release notes: https://github.com/Pennsieve/pennsieve-python/blob/master/CHANGELOG.md
