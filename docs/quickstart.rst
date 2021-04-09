Getting Started in Python
=========================

Python client and command line tool for Pennsieve.

Installation
------------

The Python client is compatible with Python 2.7 and 3.4-3.7.

.. code:: bash

    $ pip install -U pennsieve

The **Pennsieve Agent** is installed separately and is required for optimal performance when uploading files.
See platform-specific installation instructions `here <https://developer.pennsieve.io/agent/>`_.

Configuration
-------------

.. important::

    In order to conect to Pennsieve using *any* client, first you must
    `Generate an API token & secret <http://help.pennsieve.com/pennsieve-developer-tools/overview/creating-an-api-key-for-the-pennsieve-clients>`_.
    Once you have generated your API keys, don't close your browser window until
    you have used your keys in the following steps.

To create a configuration profile, run ``pennsieve-profile create`` from the command line:

.. code:: bash

    $ pennsieve-profile create

When prompted, give your profile a unique name, or press enter to name your profile ``default``:

.. code:: bash

   Profile name [default]: my_profile

When prompted, paste in your new API key (also called a token) and press enter:

.. code:: bash

   API token: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx

Now paste in the API secret key and press enter:

.. code:: bash

   API secret: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx

Finally, enter ``y`` to set this profile as the *default profile*:

.. code:: bash

   Would you like to set 'my_profile' as default (Y/n)? y
   Default profile: my_profile

To verify that your profile was set up correctly, run ``pennsieve-profile status``:

.. code:: bash

   $ pennsieve-profile status

   Active profile:
     my_profile

   Pennsieve environment:
     User          : <your email>
     Organization  : <your organization>
     API Location  : https://api.pennsieve.io

Using this technique you can add multiple connection profiles belonging to different organizations.

Basic Usage
--------------

Import and Initialize
~~~~~~~~~~~~~~~~~~~~~~

.. note::

   If you are using Python 2.7 it is highly recommended that you add
   ``from __future__ import print_function`` to the top of your scripts. This will
   allow you to easily use Python 3 in the future.

.. code:: python

    from pennsieve import Pennsieve

    ps = Pennsieve()

This will use your *default profile* to establish a connection. Alternatively, you
may want to specify a profile explicitly by name:

.. code:: python

    ps = Pennsieve('my_profile')

Where ``my_profile`` is an existing profile.


Basic Operations
~~~~~~~~~~~~~~~~~~~~~~

Get your datasets::

    # print your available datasets
    for ds in ps.datasets():
        print(" Found a dataset: ", ds.name)

    # grab some dataset by name
    ds1 = ps.get_dataset('my dataset 1')

    # list items inside dataset (first level)
    print(ds1.items)

Upload some files into your dataset::

    ds1.upload('/path/to/data.pdf')

Get a data package::

    # use ID to get a package
    pkg = ps.get('N:package:1234-1234-1234-1235')

Rename it & add some properties::

    pkg.name = "My new package name"
    pkg.set_property('Temperature', 83.0)
    pkg.update()


Uploading data
----------------

.. warning::

   By default, uploads through the Python client require the Pennsieve Agent to
   be installed. See :ref:`agent` for more information on uploading data, and
   instructions on how to use the legacy uploader without the Agent.


You can upload into a ``Dataset`` or ``Collection`` using the ``.upload()`` methods::

    # upload a file into a dataset (ds)
    ds.upload('/path/to/my_data.nii.gz')

    # upload into a collection
    collection = ds.create_collection('my data folder')
    collection.upload('/path/to/my_data.mef')



Retrieving data
----------------

Let's say you grab a ``TimeSeries`` package::

    ts = ps.get('N:package:your-timeseries-id')

You can get the first minute of data in 1-second chunks::

    for chunk in ts.get_data_iter(chunk_size='1s', length='1m'):
        # do something with data (pandas Dataframe)
        print("Mean values =", chunk.mean())

You can do the same thing for a single channel::

    channel = ts.channels[0]
    for chunk in channel.get_data_iter(chunk_size='5s', length='10m'):
        # do something with data (pandas Series)
        print("Max value =", chunk.max())
