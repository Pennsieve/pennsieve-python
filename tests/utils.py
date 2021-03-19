""" Utility functions for generating test fixtures """

import time
from uuid import uuid4

from pennsieve import Pennsieve


def current_ts():
    """ Gets current timestamp """
    return int(round(time.time() * 1000))


def get_test_client(profile=None, api_token=None, api_secret=None, **overrides):
    """ Utility function to get a Pennsieve client object """
    ps = Pennsieve(
        profile=profile, api_token=api_token, api_secret=api_secret, **overrides
    )
    assert ps.context is not None
    orgs = ps.organizations()
    assert len(orgs) > 0

    # explicitly set context to Blackfyn org
    assert ps.context is not None

    return ps


def create_test_dataset(ps_client):
    """Utility function to generate a dataset for testing. It is up to the
    caller to ensure the dataset is cleaned up
    """
    ds = ps_client.create_dataset("test_dataset_{}".format(uuid4()))
    ds_id = ds.id
    # Removing this check to limit the number of API calls:
    # all_dataset_ids = [x.id for x in ps_client.datasets()]
    # assert ds_id in all_dataset_ids
    return ds
