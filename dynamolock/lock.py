'''
The DynamoDBLock represents a single instance of a lock along with the
relevant information needed track its state. It is an immutable tuple
so that the rest of the system will be thread safe. Thus, the instances
of lock that are returned from the client have no impact on the function
of the client.
'''
from collections import namedtuple

#--------------------------------------------------------------------------------
# classes
#--------------------------------------------------------------------------------

DynamoDBLock = namedtuple('DynamoDBLock',
    ['name', 'version', 'owner', 'duration', 'timestamp', 'is_locked', 'payload'])
