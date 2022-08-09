"""
Pennsieve Configuration File
----------------------------

Pennsieve stores connection information in your Pennsieve configuration file.
Advanced users might want to edit their Pennsieve client tool configuration file directly
for control of the client libraries. Alternatively, users can modify client behavior using
environment variables.

Location
~~~~~~~~

Your configuration file is located in your ``.pennsieve/`` directory. The ``.pennsieve/`` directory
is found in the ``$HOME`` directory (Mac/Linux) or the ``User`` directory (Windows).

Full path: ``$HOME/.pennsieve/config.ini``

Format
~~~~~~

The config file is in `INI <https://en.wikipedia.org/wiki/INI_file>`_ format.
There are three types of sections: ``[global]``, ``[agent]``, and ``[<profile>]``.
You can have as many ``[<profile>]`` sections as you want.

Example of the ``config.ini`` file:

.. code-block:: ini

    # Global settings
    [global]
    default_profile = default

    # Profiles
    [default]
    api_token = c09be34d-5696-4c49-b174-7fe3fb3194af
    api_secret = 87092fd-b3ad-4de9-ps78-2dbcedb7737a

    [debug_mode]
    use_cache = false
    api_token = c09be34d-5696-4c49-b174-7fe3fb3194af
    api_secret = 87092fd-b3ad-4de9-ps78-2dbcedb7737a

    [super_conn]
    api_token = da064188-47e4-43b0-b5cd-91805b7522d7
    api_secret = 2a543888-d24d-4958-8833-3311a55e4ed6

    # Settings for the Pennsieve CLI Agent
    [agent]
    ...

The following settings (and their default values) are available under ``[<profile>]`` or ``[global]``:

.. code-block:: ini

    # Pennsieve API token/secret
    'api_token'                   : None,
    'api_secret'                  : None,

    'api_host'                    : 'https://api.pennsieve.io',

    # I/O
    'max_request_time'            : 120, # two minutes
    'max_request_timeout_retries' : 2,
    'max_upload_workers'          : 10,

    # Timeseries
    'max_points_per_chunk'        : 10000,

    # Directories
    'pennsieve_dir'               : $HOME/.pennsieve
    'cache_dir'                   : $HOME/.pennsieve/cache

    # Cache
    'cache_index'                 : $HOME/.pennsieve/cache/index.db
    'cache_max_size'              : 2048,
    'cache_inspect_interval'      : 1000,
    'ts_page_size'                : 3600,
    'use_cache'                   : True,

In addition to the above, these settings are available under ``[global]``:

.. code-block:: ini

    default_profile

To see your current configuration (and any variables), use the Python command line tool:

.. code-block:: bash

    $ ps profile keys

Environment Variables
---------------------

It is also possible to set configuration options using environment variables

.. note:

    Environment variables (if present) override any profile-defined settings
    in your Pennsieve Configuration File. They are useful for terminal-specific settings.

To switch between profiles in a given terminal session, set the environment variable:

.. code-block:: bash

    PENNSIEVE_PROFILE="your profile name"

Alternatively, you can specify your token/secret directly:

.. code-block:: bash

    PENNSIEVE_API_TOKEN="xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
    PENNSIEVE_API_SECRET="xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"

To control the verbosity of the Python client's logging:

.. code-block:: bash

    PENNSIEVE_LOG_LEVEL=("DEBUG" or "INFO" or "WARN" or "ERROR")

To specify an alternate directory to use as the Pennsieve config directory:

.. code-block:: bash

    PENNSIEVE_DIR="./some_other_dir"

If the ``PENNSIEVE_DIR`` environment variable is set, your configuration file will be:

.. code-block:: bash

    $PENNSIEVE_DIR/config.ini

Additional environment variables and their corresponding config options:

.. code-block:: bash

    PENNSIEVE_USE_CACHE: 0  (false) or 1  (true)  # `use_cache`
    PENNSIEVE_API_LOC                             # `api_host`
    PENNSIEVE_CACHE_MAX_SIZE                      # `cache_max_size`
    PENNSIEVE_CACHE_INSPECT_EVERY                 # `cache_inspect_interval`
    PENNSIEVE_TS_PAGE_SIZE                        # `ts_page_size`

"""

from __future__ import absolute_import, division, print_function

import configparser
import os
from warnings import warn

PENNSIEVE_DIR_DEFAULT = os.path.join(os.path.expanduser("~"), ".pennsieve")
CACHE_DIR_DEFAULT = os.path.join(PENNSIEVE_DIR_DEFAULT, "cache")
CACHE_INDEX_DEFAULT = os.path.join(CACHE_DIR_DEFAULT, "index.db")

DEFAULTS = {
    # pennsieve api locations
    "api_host": "https://api.pennsieve.io",
    "model_service_host": None,
    # Pennsieve AWS Cognito Client Application ID
    # pennsieve API token/secret
    "api_token": None,
    "api_secret": None,
    # global headers
    "headers": None,
    # all requests
    "max_request_time": 120,  # two minutes
    "max_request_timeout_retries": 2,
    # io
    "max_upload_workers": 10,
    # timeseries
    "max_points_per_chunk": 10000,
    # s3 (amazon/local)
    "s3_host": "",
    "s3_port": "",
    # directories
    "pennsieve_dir": PENNSIEVE_DIR_DEFAULT,
    "cache_dir": CACHE_DIR_DEFAULT,
    # cache
    "cache_index": CACHE_INDEX_DEFAULT,
    "cache_max_size": 2048,
    "cache_inspect_interval": 1000,
    "ts_page_size": 3600,
    "use_cache": True,
}

ENVIRONMENT_VARIABLES = {
    "api_host": ("PENNSIEVE_API_LOC", str),
    "api_token": ("PENNSIEVE_API_TOKEN", str),
    "api_secret": ("PENNSIEVE_API_SECRET", str),
    "pennsieve_dir": ("PENNSIEVE_LOCAL_DIR", str),
    "cache_dir": ("PENNSIEVE_CACHE_LOC", str),
    "cache_max_size": ("PENNSIEVE_CACHE_MAX_SIZE", int),
    "cache_inspect_interval": ("PENNSIEVE_CACHE_INSPECT_EVERY", int),
    "ts_page_size": ("PENNSIEVE_TS_PAGE_SIZE", int),
    "use_cache": ("PENNSIEVE_USE_CACHE", lambda x: bool(int(x))),
    "default_profile": ("PENNSIEVE_PROFILE", str),
    # advanced
    "s3_host": ("S3_HOST", str),
    "s3_port": ("S3_PORT", str),
}


class Settings(object):
    def __init__(self, profile=None, overrides=None, env_override=True):
        # hydrate with standard defaults first
        self._update(DEFAULTS)

        # load and apply environment variables
        environs = self._load_env()

        # check and create pennsieve directory so that we can load config file
        if not os.path.exists(self.pennsieve_dir):
            os.makedirs(self.pennsieve_dir)

        self.profiles = {}
        self._load_config()
        self._load_profiles()

        # use default profile first
        try:
            # first apply config default profile
            self._switch_profile(self.config["global"]["default_profile"])
        except BaseException:
            self._switch_profile("global")

        # apply PENNSIEVE_PROFILE
        self._switch_profile(environs.get("default_profile"))

        # use specific profile if specified
        self._switch_profile(profile)

        # override with env variables
        if env_override:
            self._update(environs)

        # update with override values passed into settings
        self._update(overrides)

        # check and create cache dir
        if not os.path.exists(self.cache_dir) and self.use_cache:
            os.makedirs(self.cache_dir)
        warn(
            f"Pennsieve is transitioning to the new agent. This class '{self.__class__.__name__}' will be deprecated; version=7.0.0; date=2022-11-01.",
            DeprecationWarning,
            stacklevel=2,
        )

    def _load_env(self):
        override = {}
        self.env = {}
        for key, (evar, typefunc) in ENVIRONMENT_VARIABLES.items():
            value = os.environ.get(evar, None)
            if value is not None:
                v = typefunc(value)
                self.env[evar] = v
                override[key] = v
        # apply envs
        self._update(override)
        return override

    def _load_config(self):
        self.config_file = os.path.join(self.pennsieve_dir, "config.ini")
        self.config = configparser.ConfigParser()
        self.config.read(self.config_file)

    # _update safely updates the internal __dict__
    def _update(self, settings):
        if settings is None or not isinstance(settings, dict):
            return
        for k in DEFAULTS:
            if k in settings:
                self.__dict__[k] = settings[k]

    def _load_profiles(self):
        # load global first
        self.profiles["global"] = DEFAULTS.copy()
        if "global" in self.config:
            self._parse_profile("global")
        for name in self.config.sections():
            if name != "global":
                self.profiles[name] = self.profiles["global"].copy()
                self._parse_profile(name)

    def _parse_profile(self, name):
        for key, value in self.config[name].items():
            if value == "none":
                self.profiles[name][key] = None
            elif value.lower() == "true":
                self.profiles[name][key] = True
            elif value.lower() == "false":
                self.profiles[name][key] = False
            elif value.isdigit():
                self.profiles[name][key] = int(value)
            else:
                self.profiles[name][key] = str(value)

    def _switch_profile(self, name):
        if name is None:
            return
        if name not in self.profiles:
            raise Exception("Invalid profile name")
        else:
            self.__dict__.update(self.profiles[name])
            self.active_profile = name
            if name == "global":
                self.active_profile = None

    @property
    def host(self):
        return self.api_host


settings = Settings()
