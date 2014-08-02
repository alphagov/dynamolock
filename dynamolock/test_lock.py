#!/usr/bin/env python
import unittest
from dynamolock.lock import DynamoDBLock

class DynamoDBLockTest(unittest.TestCase):

    def test_lock_init(self):
        params = {
            'name':      'my.lock.name',
            'owner':     'host.company.org.123e4567-e89b-12d3-a456-426655440000',
            'timestamp': 1406929231,
            'is_locked': True,
            'duration':  5000000,
            'version':  '123e4567-e89b-12d3-a456-426655440000',
            'payload':   None,
        }
        lock = DynamoDBLock(**params)
        self.assertIsNotNone(lock)

    def test_lock_copy(self):
        params = {
            'name':      'my.lock.name',
            'owner':     'host.company.org.123e4567-e89b-12d3-a456-426655440000',
            'timestamp': 1406929231,
            'is_locked': True,
            'duration':  5000000,
            'version':  '123e4567-e89b-12d3-a456-426655440000',
            'payload':   None,
        }
        old_lock = DynamoDBLock(**params)
        new_lock = old_lock._replace(owner='another.company.org.123e4567-e89b-12d3-a456-426655440000')

        self.assertIsNotNone(old_lock)
        self.assertIsNotNone(new_lock)
        self.assertNotEqual(old_lock, new_lock)

#---------------------------------------------------------------------------#
# main
#---------------------------------------------------------------------------#
if __name__ == "__main__":
    unittest.main()
