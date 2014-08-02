import time
from threading import Thread, Event
from datetime import timedelta

from .policy import DynamoDBLockPolicy

#--------------------------------------------------------------------------------
# logging
#--------------------------------------------------------------------------------

import logging
_logger = logging.getLogger(__name__)

#--------------------------------------------------------------------------------
# classes
#--------------------------------------------------------------------------------

class DynamoDBLockWorker(Thread):
    ''' The worker that runs to periodically update lock leases as long
    as the system is alive. This prevents long running processes from
    losing their locks by possibly fast clients.

    .. code-block:: python

        from dynamodb import DynamoDBLockWorker
        from dynamodb import DynamoDBLockClient

        # Note, this is actually all internal to the client,
        # do not do this.
        client = DynamoDBLockClient() 
        worker = DynamoDBLockWorker(client=client)
        worker.start()
        worker.stop(timeout=10) # seconds
    '''

    def __init__(self, **kwargs):
        ''' Initializes a new instance of the DynamoDBLock class

        :param daemon: True to daemonize the thread, False otherwise (default True)
        :param client: The client to perform management with
        :param policy: The policy to operate the worker with
        :param locks: The dictionary of locks to manage (default the client locks)
        :param period: The length of each cycle in seconds (default 1 minutes)
        '''
        super(DynamoDBLockWorker, self).__init__()

        self.daemon = kwargs.get('daemon', True)
        self.client = kwargs.get('client')
        self.policy = kwargs.get('policy', DynamoDBLockPolicy())
        self.locks  = kwargs.get('locks', self.client.locks)
        self.period = kwargs.get('period', timedelta(seconds=10).total_seconds())
        self._is_stopped = Event()

    def stop(self, timeout=None):
        ''' Stop the underlying worker thread and join on its
        completion for the specified timeout.

        :param timeout: The amount of time to wait for the shutdown
        '''
        self._is_stopped.set()
        if self.is_alive(): self.join(timeout)

    def run(self): 
        ''' The worker thread used to update the lock leases
        for the currently handled locks.
        '''
        while not self._is_stopped.is_set():
            _logger.debug("starting next round of worker: %d locks", len(self.locks))
            start = self.policy.get_new_timestamp()
            for lock in self.locks.values():
                if not self.client.touch_lock(lock):
                    del self.locks[lock.name]
            elapsed = (self.policy.get_new_timestamp() - start) / 1000
            time.sleep(max(self.period - elapsed, 0))
