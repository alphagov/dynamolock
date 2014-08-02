import time
import uuid
import socket
import json
from datetime import timedelta

#--------------------------------------------------------------------------------
# logging
#--------------------------------------------------------------------------------

import logging
_logger = logging.getLogger(__name__)

#--------------------------------------------------------------------------------
# classes
#--------------------------------------------------------------------------------

class DynamoDBLockPolicy(object):
    '''
    Along with the timing policy, this class also includes the policy
    for getting a new version, new timestamp, new owner identifer,
    and checking if a lock name is valid. All of these can be
    overridden to customize the use case for the system::

        import os
        import time
        from dynamolock import DynamoDBLockPolicy

        class MyPolicy(DynamoDBLockPolicy):

            def is_name_valid(self, name):
                return name.startswith("application.")

            def get_new_owner(self):
                return "%s:%d" % (os.getenv('HOST'), os.getpid())

            def get_new_version(self):
                return time.time()
    '''

    def __init__(self, **kwargs):
        ''' Initailize a new instance of the DynamoDBLockPolicy class

        :param acquire_timeout: The amount of time to wait trying to get a lock
        :param retry_period: The time to wait between retries to the server
        :param lock_duration: The default amount of time needed to hold the lock
        :param delete_lock: True to delete locks on release, false otherwise
        '''
        acquire_timeout  = kwargs.get('acquire_timeout', timedelta(seconds=10))
        retry_period     = kwargs.get('retry_period', timedelta(seconds=10))
        lock_duration    = kwargs.get('lock_duration', timedelta(minutes=1))
        self.delete_lock = kwargs.get('delete_lock', True)

        self.acquire_timeout = long(acquire_timeout.total_seconds() * 1000)
        self.retry_period    = long(retry_period.total_seconds())
        self.lock_duration   = long(lock_duration.total_seconds() * 1000)

    def is_name_valid(self, name):
        ''' Helper method to check if the supplied name is valid
        to use as a key or not.

        :param name: The name to check for validity
        :returns: True if a valid name, False otherwise
        '''
        return bool(name)

    def get_new_owner(self):
        ''' Helper method to retrieve a new owner name that is
        not only unique to the server, but unique to the application
        on this server.

        :returns: A new owner name to operate with
        '''
        return "%s.%s" % (socket.gethostname(), uuid.uuid4())

    def get_new_version(self):
        ''' Helper method to retrieve a new version number
        for a lock. This can be overloaded to provide a custom
        protocol.

        :returns: A new version number
        '''
        return str(uuid.uuid4())

    def get_new_timestamp(self):
        ''' Helper method to retrieve the current time since
        the epoch in milliseconds.

        :returns: The current time in milliseconds
        '''
        return long(time.time() * 1000)

    # ------------------------------------------------------------
    # magic methods
    # ------------------------------------------------------------

    def __str__(self):
        return json.dumps(self.__dict__)

    __repr__ = __str__
