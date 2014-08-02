================================================================================
Dynamolock
================================================================================

Dynamolock is a a distributed lock client on top of dynamo written in python. As
long as clients use the library and respect the protocol, it provides gurantees
that a lock will not be taken unless:

1. the lock is explicity unlocked by its owner
2. the lock is delete by its owner
3. the lock has timed out locally

Furthermore, if any operation interleaves when we determine that we are okay to
perform a lock operation and when we actually make that operation, it is prevented
by using check-and-set(CAS) on the owner and version number of the lock.

To test if a lock is timed out, the lock in question stores a duration field in
dynamo. When a client tries to acquire the lock, it must first wait at least that
amount of time locally (now() + duration) before it is allowed to take over the
lock. This protocol ensures that work can progress even in the face of failed
clients without having to distribute up-status between clients.

For more information, please read the documentation at:
https://readthedocs.org/projects/dynamolock/
