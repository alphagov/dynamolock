from time import sleep
from copy import copy

from boto.exception import JSONResponseError
from boto.dynamodb2.types import Dynamizer
from boto.dynamodb2.fields import HashKey
from boto.dynamodb2.types import STRING
from boto.dynamodb2.items import Item
from boto.dynamodb2.table import Table
from boto.dynamodb2.exceptions import ConditionalCheckFailedException, ItemNotFound

from .lock   import DynamoDBLock
from .policy import DynamoDBLockPolicy
from .schema import DynamoDBLockSchema
from .worker import DynamoDBLockWorker

#--------------------------------------------------------------------------------
# logging
#--------------------------------------------------------------------------------

import logging
_logger = logging.getLogger(__name__)

#--------------------------------------------------------------------------------
# classes
#--------------------------------------------------------------------------------

class DynamoDBLockClient(object):

    def __init__(self, **kwargs):
        ''' Initialize a new instance of the DynamoDBLockClient class

        :param locks: The collection of locks to watch, default {}
        :param policy: The timing policy for taking and timing out locks
        :param schema: The schema of the database table to work with
        :param owner: The owner of the locks created by this client
        :param table: The current handle to the dynamodb table client
        :param worker: The underlying heartbeat worker to work with
        '''
        self.locks  = kwargs.get('locks', {})
        self.policy = kwargs.get('policy', DynamoDBLockPolicy())
        self.schema = kwargs.get('schema', DynamoDBLockSchema())
        self.owner  = kwargs.get('owner', self.policy.get_new_owner())
        self.table  = kwargs.get('table', None) or self._create_table()
        self.worker = kwargs.get('worker', DynamoDBLockWorker(client=self))

    # ------------------------------------------------------------
    # worker methods
    # ------------------------------------------------------------

    def startup(self):
        ''' Start the heartbeat thread and perform any lock
        initialization.
        '''
        self.worker.start()

    def shutdown(self):
        ''' Stop the heartbeat thread and close all of the existing
        lock handles that we have outstanding leases to.
        '''
        self.worker.stop(timeout=self.policy.retry_period)
        self.release_all_locks()

    # ------------------------------------------------------------
    # lock validation methods
    # ------------------------------------------------------------

    def is_lock_expired(self, lock):
        ''' Given a lock, test if it is expired or not. This is
        equivalent to to `not is_lock_active(lock)`.

        :param lock: The lock to check if it is expired
        :returns: True if the lock is expired, False otherwise
        '''
        expired_timestamp = lock.timestamp + lock.duration
        return self.policy.get_new_timestamp() > expired_timestamp

    def is_lock_active(self, lock):
        ''' Given a lock, test if it the lock is still active
        based on our current time information. This is equivalent
        to `not is_lock_expired(lock)`.

        :param lock: The lock to check if it is active
        :returns: True if the lock is active, False otherwise
        '''
        active_timestamp = lock.timestamp + lock.duration
        return  self.policy.get_new_timestamp() <= active_timestamp

    def is_lock_valid(self, lock):
        ''' Given a lock, test if it is still a valid lock for
        the current system owner:

        * valid name
        * is still locked
        * is owned by the current owner
        * is not expired

        :param lock: The lock to check for liveness
        :returns: True if the lock is alive, False otherwise
        '''
        return ((self.policy.is_name_valid(lock))
            and (lock.is_locked)
            and (lock.owner == self.owner)
            and (self.is_lock_active(lock)))

    # ------------------------------------------------------------
    # locking manipulation methods
    # ------------------------------------------------------------

    def touch_lock(self, lock):
        ''' Touch the lock and update its version to renew the
        lease we are currently holding on the lock (if we can).

        :param lock: The lock to attempt to touch
        :returns: The new lock if it was updated, None otherwise
        '''
        if not self.is_lock_valid(lock):
            _logger.debug("failed touching invalid lock:\n%s", str(lock))
            return None

        new_lock = self._update_entry(lock)
        if new_lock:
            _logger.debug("success touching lock:\n%s", str(lock))
            self.locks[lock.name] = new_lock
        return new_lock

    def release_lock(self, lock, delete=None, **params):
        ''' Release the supplied lock and apply any supplied udpates
        to the underlying name.
        
        If the delete flag is not set, it will default to the currently
        installed policy value `delete_lock`.

        :param lock: The lock to attempt to release
        :param delete: True to also delete locks, False to mark them unlocked
        :returns: True if the lock was released, False otherwise
        '''
        if not self.is_lock_valid(lock):
            _logger.debug("failed releasing invalid lock:\n%s", str(lock))
            return False

        delete = delete if (delete != None) else self.policy.delete_lock

        # ------------------------------------------------------------
        # Case 1:
        # ------------------------------------------------------------
        # If we do not delete the lock, we simply update the is_locked
        # flag as long as we are still the owner of the lock at the
        # lock version is still what we expect it to be.
        # ------------------------------------------------------------
        if not delete:
            params['is_locked'] = False
            new_lock = self._update_entry(lock, update=params)
            is_released = bool(new_lock)

        # ------------------------------------------------------------
        # Case 2:
        # ------------------------------------------------------------
        # If we do delete the lock, we simply remove it from the
        # database as long as the version number has not changed.
        # Otherwise we simply fail.
        # ------------------------------------------------------------
        else: is_released = self._delete_entry(lock)

        # ------------------------------------------------------------
        # Cleanup:
        # ------------------------------------------------------------
        # After releasing the lock, we remove it from our cache only
        # if we did in fact release the lock.
        # ------------------------------------------------------------
        if is_released and (lock.name in self.locks):
            del self.locks[lock.name]
        return is_released

    def release_all_locks(self, delete=None, **params):
        ''' Release all the currently held locks by this instance of
        the lock client (cached).
        
        All the supplied params that are applicable are passed on to the
        underlying operation.

        :param delete: True to also delete locks, False to mark them unlocked
        :returns: True if all locks were released, False otherwise
        '''
        locks    = self.locks.values()
        released = [self.release_lock(lock, delete, **params) for lock in locks]
        return all(released) # so we don't short circuit any evaluation

    def acquire_lock(self, name, no_wait=False, **params):
        ''' Attempt to acquire the lock with the paramaters
        specified in the initial lock policy.
        
        All the supplied params that are applicable are passed on
        to the underlying operation.

        :param name: The name of the lock to acquire
        :param no_wait: Try to acquire the lock without waiting
        :returns: The acquired lock on success, or None
        '''
        if not self.policy.is_name_valid(name):
            return None

        initial_time   = self.policy.get_new_timestamp() # the time we started trying to acquire
        lock_timeout   = self.policy.acquire_timeout     # how long to wait until we fail
        refresh_time   = self.policy.retry_period        # how long to wait between database reads
        waited_time    = 0                               # the total amount of time we have waited
        watching_lock  = None                            # the watch we are currently trying to get
        created_lock   = None                            # the watch that we created and is valid
        tried_one_time = False                           # indicates if we have made one attempt at the lock

        while (self.policy.get_new_timestamp() < (initial_time + lock_timeout)
           or (no_wait and tried_one_time)):             # if the user wants to try_acquire
            current_lock = self._retrieve_entry(name)

            # ------------------------------------------------------------
            # Case 1:
            # ------------------------------------------------------------
            # There is no existing lock in the database, so we can simply
            # grab the lock if we are able to, otherwise we loop and try
            # again.
            # ------------------------------------------------------------
            if not current_lock:
                created_lock = self._create_entry(name, **params)

            # ------------------------------------------------------------
            # Case 2:
            # ------------------------------------------------------------
            # There is an existing lock in the database, however, it has
            # already been unlocked and exists because a previous user
            # chose not to delete it or failed to do so. Regardless, we
            # can simply overwrite the lock and make use of the existing
            # data if we so choose.
            # ------------------------------------------------------------
            elif not current_lock.is_locked:
                params['owner'] = self.owner
                expect = ['is_locked', 'version', 'name']
                created_lock = self._update_entry(current_lock, expect=expect, update=params)

            # ------------------------------------------------------------
            # Case 3:
            # ------------------------------------------------------------
            # If we are currently watching a lock and it has locally
            # become expired (we have waited the specified lease of the
            # lock) and the version has not changed in the interum, we
            # are allowed to take control of the lock if we can.
            # ------------------------------------------------------------
            elif (watching_lock
             and (self.is_lock_expired(watching_lock))
             and (watching_lock.version == current_lock.version)):
                params['owner'] = self.owner
                expect = ['version', 'name']
                created_lock = self._update_entry(current_lock, expect=expect, update=params)

            # ------------------------------------------------------------
            # Case 4:
            # ------------------------------------------------------------
            # If we are currently not watching a lock, but someone has
            # the lock that we want, we start watching it and update our
            # timeout to match the lease of the lock.
            # ------------------------------------------------------------
            elif not watching_lock:
                lock_timeout += current_lock.duration
                watching_lock = current_lock

            # ------------------------------------------------------------
            # Case 5:
            # ------------------------------------------------------------
            # If we are currently watching a lock and waiting for it to
            # expire and someone has gotten a new lease on that lock in
            # the interum between our delay, then we are forced to watch
            # the new lock. However, we do not update our delay time as
            # we might otherwise wait forever.
            # ------------------------------------------------------------
            elif (watching_lock
             and (watching_lock.version != current_lock.version)):
                watching_lock = current_lock

            # ------------------------------------------------------------
            # Cleanup:
            # ------------------------------------------------------------
            # If we were able to create a lock, then we add it to our
            # cache, update its timestamp locally, and return a copy to
            # the system so they cannot modify our state. Next, if we
            # plan on waiting for the lock, we sleep until the next retry
            # period. Otherwise, if we refused to wait for the lock, we
            # simply exit if we failed.
            # ------------------------------------------------------------
            if created_lock:
                self.locks[name] = created_lock
                return created_lock
            elif not no_wait:
                _logger.debug("waiting %d secs to acquire lock %s, total wait %d secs", refresh_time, name, waited_time)
                sleep(refresh_time)
                waited_time += refresh_time
            else: tried_one_time = True

        # ------------------------------------------------------------
        # Failure:
        # ------------------------------------------------------------
        # If after waiting the supplied buffer time plus the original
        # lock duration we still were not able to get a lock handle,
        # we simply fail and let the user know.
        # ------------------------------------------------------------
        return None

    def try_acquire_lock(self, name, **params):
        ''' Attempt to acquire the lock without waiting, instead
        simply fail fast.
        
        All the supplied params that are applicable are passed on
        to the underlying operation.

        :param name: The name of the lock to acquire
        :returns: The lock on success, None on failure
        '''
        return self.acquire_lock(name, no_wait=True, **params)

    def does_lock_exist(self, name):
        ''' Check if a lock with the given name exists on the
        backend database and is active.

        :param name: The name of the lock to check for existance
        :returns: True if the lock exists, False otherwise
        '''
        return bool(self.retrieve_lock(name))

    def retrieve_lock(self, name):
        ''' Retrieve the lock by the supplied name strictly
        to view its data, but not to perform any updates.

        :param name: The lock name to retrieve
        :returns: The lock at the supplied name or None
        '''
        if not self.policy.is_name_valid(name):
            _logger.debug("cannot retrieve lock with invalid name: %s", name)
            return None

        # ------------------------------------------------------------
        # Case 1:
        # ------------------------------------------------------------
        # We try the cache first to see if we are already watching
        # that lock and already have a timestamp running.
        # ------------------------------------------------------------
        if name in self.locks:
            current_lock = self.locks[name]

        # ------------------------------------------------------------
        # Case 2:
        # ------------------------------------------------------------
        # We are not watching that lock, so we pull down a fresh copy
        # and return it to the user. However, we do not cache this
        # lock as we are not watching it.
        # ------------------------------------------------------------
        else: current_lock = self._retrieve_entry(name)

        # ------------------------------------------------------------
        # Cleanup:
        # ------------------------------------------------------------
        # We clear any information # that would allow the user to
        # modify this lock instance (the version) as they are not
        # watching this lock. Also, if the lock is stale (unlocked)
        # we treat that lock as not existing.
        # ------------------------------------------------------------
        if current_lock:
            current_lock = current_lock._replace(version=None)
            if not current_lock.is_locked: current_lock = None

        return current_lock

    # ------------------------------------------------------------
    # raw dynamo methods
    # ------------------------------------------------------------

    def _create_table(self):
        ''' Create the underlying dynamodb table for writing
        locks to if it does not exist, otherwise uses the existing
        table. We use the `describe` method call to verify if the
        table exists or not.

        :returns: A handle to the underlying dynamodb table
        '''
        try:
            table = Table(self.schema.table_name)
            _logger.debug("current table description:\n%s", table.describe())
        except JSONResponseError, ex:
            _logger.exception("table %s does not exist, creating it", self.schema.table_name)
            table = Table.create(self.schema.table_name,
                schema = [ HashKey(self.schema.name, data_type=STRING) ],
                throughput = {
                    'read':  self.schema.read_capacity,
                    'write': self.schema.write_capacity,
                })
            _logger.debug("current table description:\n%s", table.describe())
        return table

    def _retrieve_entry(self, name):
        ''' Given the name of a lock, attempt to retrieve the
        lock and update its value in the cache.

        :param name: The name of the lock to retrieve
        :returns: The lock if it exists, None otherwise
        '''
        query  = {
            self.schema.name: name,
            'consistent': True,
        }

        try:
            record = self.table.get_item(**query)
            params = self.schema.to_dict(record)
            params['timestamp'] = self.policy.get_new_timestamp()
            return DynamoDBLock(**params)
        except ItemNotFound, ex:
            _logger.exception("failed to retrieve item: %s", name)
        return None

    def _delete_entry(self, lock):
        ''' Attempt to delete the lock from dynamodb with
        the supplied name.
        
        We only allow a lock to be deleted if we know the
        current version number and we are the current owner
        of the lock. In order to delete another users lock, we
        must update it such that we are the owner.

        :param lock: The lock to delete
        :returns: True if successful, False otherwise
        '''
        expected = { 'name': lock.name, 'version': lock.version }
        expected = self.schema.to_schema(expected)
        expected = { '%s__eq' % key : val for key, val in expected.items() }
        params   = { self.schema.name : lock.name }

        try:
            return self.table.delete_item(expected=expected, **params)
        except ConditionalCheckFailedException, ex:
            _logger.exception("failed to delete item: %s", name)
        return False

    def _create_entry(self, name, **params):
        ''' Attempt to update the underlying lock on dynamodb
        with the supplied values.
        
        All the supplied params that are applicable are passed
        on to the underlying operation, although all but payload
        will simply be overwritten.

        :param name: The name of the lock to update
        :returns: True if successful, False otherwise
        '''
        params.update({
            'name':      name,
            'owner':     self.owner,
            'version':   self.policy.get_new_version(),
            'duration':  params.get('duration', self.policy.lock_duration),
            'timestamp': self.policy.get_new_timestamp(),
            'is_locked': True,
        })

        # ------------------------------------------------------------
        # Case 1:
        # ------------------------------------------------------------
        # We have to make sure that no one beat us in creating an
        # entry at this specified key, otherwise we should fail.
        # ------------------------------------------------------------
        expects = { self.schema.name: { 'Exists' : "false" } }
        record  = self.schema.to_schema(params)
        record  = self.table._encode_keys(record)

        try:
            self.table._put_item(record, expects=expects)
            if 'payload' not in params: params['payload'] = None
            return DynamoDBLock(**params)
        except (JSONResponseError, ConditionalCheckFailedException):
            _logger.exception("failed to create lock entry for: %s", name)
        return None

    def _update_entry(self, lock, expect=None, update=None):
        ''' Attempt to update the underlying lock on dynamodb
        with the supplied values.

        In order to update an entry, at least the version number
        must be the same. If we are overwriting a timed out lock
        we change ourself to the new owner, otherwise, we must be
        the current owner to update.

        Every update will automatically bump the version number
        of the underlying lock so clients can refresh their lease.

        :param lock: The lock to update
        :param expect: A list of fields you expect to not have changed
        :param update: A dictionary of fields to update
        :returns: True if successful, False otherwise
        '''
        version = self.policy.get_new_version()
        name    = { self.schema.name : lock.name }

        updates = { 'version' : version, 'timestamp': self.policy.get_new_timestamp() }
        if update: updates.update(update)
        updated = self.schema.to_schema(updates)
        updated = self.table._encode_keys(updated)
        updated = { k : { 'Value': v, 'Action': 'PUT' } for k, v in updated.items() }

        expects = expect or ['version', 'owner', 'name']
        expects = { key : getattr(lock, key) for key in expects }
        expects = self.schema.to_schema(expects)
        expects = self.table._encode_keys(expects)
        expects = { k : { 'Value': v } for k, v in expects.items() }

        try:
            self.table._update_item(name, updated, expects=expects)
            return lock._replace(**updates)
        except ConditionalCheckFailedException:
            _logger.exception("failed to create lock entry for: %s", name)
        return None
