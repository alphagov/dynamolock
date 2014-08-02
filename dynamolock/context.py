#--------------------------------------------------------------------------------
# logging
#--------------------------------------------------------------------------------

import logging
_logger = logging.getLogger(__name__)

#--------------------------------------------------------------------------------
# classes
#--------------------------------------------------------------------------------

class DynamoDBLockContext(object):
    ''' A context manager to help using locks in a `with` statement.

    .. code-block:: python

        from dynamolock import DynamoDBLockContext as locker

        with locker(client=client, name="lock-to-get") as handle:
            pass # perform locked activity here
        # upon leaving the lock will be removed
    '''

    def __init__(self, **kwargs):
        ''' Initialize a new instance of the DynamoDBLockContext

        :param client: The client to acquire the lock with
        :param name: The name of the lock to acquire
        '''
        self.client = kwargs.get('client')
        self.name   = kwargs.get('name')

    def __enter__(self):
        ''' On enter of the context manager, this will acquire
        the specified lock.  When the lock has been acquired,
        this will return.
        '''
        self.lock = self.client.acquire_lock(name)
        return self

    def __exit__(self, ex_type, value, traceback):
        ''' On exit of the contet manager, this will release
        the currently being held lock. When this operation is
        finished, this will return.
        '''
        self.client.release_lock(self.lock)
