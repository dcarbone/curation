# python imports
import copy
import datetime
import logging
import threading
from typing import Optional, List, Union

# google stuff
import google.cloud.exceptions
from google.cloud import storage as gcstore
from google.cloud.exceptions import NotFound as GCSNotFoundError

# our stuff
from data_steward.gcs_utils import get_hpo_bucket, storage_client
from data_steward.utils.pipeline_logging import configure as configure_log

class HPOBucket:
    """
    HPOBucket is the entrypoint for all interactions with a given HPO's google cloud bucket
    """
    def __init__(self, hpo_id: str, storage_client: Optional[gcstore.Client] = None):
        """
        :type hpo_id: str
        :param hpo_id: ID of HPO
        :type storage_client: google.cloud.storage.Client
        :param storage_client: (Optional) Google Cloud Storage client instance to utilize.
                               If not provided, one will be created using defaults using
                               ``gcs_utils.storage_client()``
        """
        # create a local mutex _just in case_
        self._lock = threading.RLock()

        # very basic input validation
        if hpo_id.strip() is '':
            raise ValueError(f'Value of hpo_id must not be empty, provided "{hpo_id}"')

        # define HPO fields
        self._hpo_id = hpo_id
        self._bucket_envvar = f'BUCKET_NAME_{hpo_id.upper()}' # TODO: better-ize this.
        self._bucket_name = get_hpo_bucket(hpo_id) # TODO: don't love this...

        # init storage client
        if storage_client is None:
            self._gcs_client = gcs_utils.storage_client()
        else:
            self._gcs_client = storage_client

        # init a few other local vars
        self._bucket = None
        self._bucket_updated = None

        # configure and build logger
        configure_log(logging.DEBUG, true)
        self._log = logging.getLogger(f'hpo_bucket.{self._bucket_name}')

    def hpo_id(self)->str:
        """
        hpo_id returns the ID of the HPO this instance operates against

        :return: str
        """
        return self._hpo_id

    def bucket_envvar_name(self)->str:
        """
        hpo_bucket_envvar_name returns the name of the envvar we expect to be defined and populated
        with the proper name of the HPO bucket

        :return: str
        """

    def bucket_name(self)->str:
        """
        bucket_name returns the name of the specific bucket in Google Cloud this instance is interacting with

        :return: str
        """
        return self._bucket_name

    def storage_client(self)->gcstore.Client:
        """
        storage_client returns the google cloud storage client utilized by this instance

        :return: google.cloud.storage.Client
        """
        return self._gcs_client

    def bucket(self, refresh: bool = False)->Optional[gcstore.bucket.Bucket]:
        """
        bucket returns the state of the HPO's bucket at the time of inquiry.

        If this class is used during a long-running process, would be a good idea to utilize
        the ``bucket_updated`` method to keep track of the last time the internal representation
        was updated, refreshing as deemed valuable.

        :return: gcstore.bucket.Bucket or None
        """
        # acquire full lock as we may be modifying the internal state of this class
        self._lock.acquire(True)
        # wrap in try / except to ensure we always unlock at the end
        try:
            # only re-open bucket if refresh=true or is not defined
            if refresh or self._bucket is None:
                bn = self.bucket_name()
                self._log.debug(f'Opening bucket {bn} with refresh={refresh}...')
                # attempt to open bucket
                self._bucket = self.storage_client().get_bucket(bn)
                self._bucket_updated = datetime.datetime.now()
        except GCSNotFoundError as err:
            self._log.error(f'Unable to open bucket {bn}: code={err.code}; msg={err.message}')
            raise err
        # TODO: other common errors to catch and log?
        finally:
            self._lock.release()

        return self._bucket or None

    def bucket_updated(self)->Optional[datetime.datetime]:
        """
        bucket_updated returns the last time the local gcs bucket representation was updated

        :return: datetime.datetime
        """
        # acquire read lock
        self._lock.acquire(False)
        up = self._bucket_updated
        self._lock.release()
        return up
