import json

#--------------------------------------------------------------------------------
# logging
#--------------------------------------------------------------------------------

import logging
_logger = logging.getLogger(__name__)

#--------------------------------------------------------------------------------
# classes
#--------------------------------------------------------------------------------

class DynamoDBLockSchema(object):
    ''' A collection of the schema names for the underlying
    locks table. This can be overridden by simply supplying
    new names in the constructor::

        from dynamolock import DynamoDBLockSchema

        schema = DynamoDBLockScema(name="key")
    '''

    def __init__(self, **kwargs):
        ''' Initializes a new instance of the DynamoDBLock class

        :param name: The database schema name for this field
        :param range_key: The database schema name for this field
        :param duration: The database schema name for this field
        :param is_locked: The database schema name for this field
        :param owner: The database schema name for this field
        :param version: The database schema name for this field
        :param payload: The database schema name for this field
        :param table_name: The name of the database locks table
        :param read_capacity: The expected read capacity for the table
        :param write_capacity: The expected write capacity for the table
        '''
        self.name           = kwargs.get('name',       'N')
        self.range_key      = kwargs.get('range_key',  'R')
        self.duration       = kwargs.get('duration',   'D')
        self.is_locked      = kwargs.get('is_locked',  'L')
        self.owner          = kwargs.get('owner',      'O')
        self.version        = kwargs.get('version',    'V')
        self.payload        = kwargs.get('payload',    'P')
        self.table_name     = kwargs.get('table_name', 'Locks')
        self.read_capacity  = kwargs.get('read_capacity', 1)
        self.write_capacity = kwargs.get('write_capacity', 1)

    # ------------------------------------------------------------
    # schema operations
    # ------------------------------------------------------------
    # These methods convert to and from the underlying table
    # schema
    # ------------------------------------------------------------

    def to_schema(self, params):
        ''' Given a dict of query params, convert them to the
        underlying schema, make sure they are valid, and remove
        paramaters that are not used.

        :param params: The paramaters to query with
        :returns: The converted query parameters
        '''
        schema = {}
        if 'name'      in params: schema[self.name]      = params['name']
       #if 'range_key' in params: schema[self.range_key] = params['range_key']
        if 'duration'  in params: schema[self.duration]  = params['duration']
        if 'is_locked' in params: schema[self.is_locked] = params['is_locked']
        if 'owner'     in params: schema[self.owner]     = params['owner']
        if 'version'   in params: schema[self.version]   = params['version']
        if 'payload'   in params: schema[self.payload]   = params['payload']
        return schema

    def to_dict(self, schema):
        ''' Given a lock record, convert it to a dict of
        the query parameter names.

        :param schema: The record to convert to a dict
        :returns: The converted dict with query parameter names
        '''
        return {
            'name'      : schema.get(self.name,      None),
        #   'range_key' : schema.get(self.range_key, None),
            'duration'  : schema.get(self.duration,  None),
            'is_locked' : schema.get(self.is_locked, None),
            'owner'     : schema.get(self.owner,     None),
            'version'   : schema.get(self.version,   None),
            'payload'   : schema.get(self.payload,   None),
        }

    def __str__(self):
        return json.dumps(self.__dict__)

    __repr__ = __str__
