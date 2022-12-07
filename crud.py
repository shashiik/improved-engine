__author__ = 'jcusick'

import binascii
import datetime
import json
import logging
import os
import time
import types
import collections
import random

from sqlalchemy.exc import SQLAlchemyError, NoSuchColumnError, IntegrityError
from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound
from sqlalchemy.orm import class_mapper

import linqia.system
from linqia.bottle.jsonutil import LinqiaJsonEncoder
from linqia.bottle.response import bottle_response
from linqia.utilities.session_scope import session_scope
from linqia.utilities.dictobj import as_dict
from linqia.utilities.db_helper import apply_filter
from linqia.models.search import TextSearch
from linqia.models.cache import CrudRecordLock
from linqia.utilities.proxy import REEF_USER_ID

CACHE_LEVEL_NAME = "X-Linqia-Cached"
CACHE_LEVEL_NOTHING = 0
CACHE_LEVEL_OVERLAY_MISS = 1
CACHE_LEVEL_OVERLAY_HIT = 2

LOCK = "lock"
RETAINLOCK = "retainlock"


class CrudResponseException(Exception):
    def __init__(self, status_code, content_type, body, headers=None):
        self.__status_code = status_code
        self.__content_type = content_type
        self.__body = body
        self.__headers = headers

        return super(CrudResponseException, self).__init__(content_type)

    def getResponse(self):
        http_headers = self.__headers if self.__headers else dict()
        http_headers['Content-Type'] = self.__content_type
        return bottle_response(self.__body, self.__status_code, http_headers)

    def getStatusCode(self):
        return self.__status_code

    def getBody(self):
        return self.__body


class CrudResponsePlain(CrudResponseException):
    def __init__(self, message, status_code=200, headers=None):
        super(CrudResponsePlain, self).__init__(status_code, 'text/plain', message, headers)


class CrudResponseJson(CrudResponseException):
    def __init__(self, body, status_code=200, headers=None):
        self.json = body
        http_body = json.dumps(body, indent=4, sort_keys=True, cls=LinqiaJsonEncoder) + '\n'
        super(CrudResponseJson, self).__init__(status_code, 'application/json', http_body, headers)


class CrudResponseResponse(CrudResponseException):
    def __init__(self, linqia_response):
        self.__response = linqia_response

    def getResponse(self):
        return self.__response


#########################################################################
# ReadObject/CrudObject parameters
#
#    sql_alchemy_model   : Primary SqlAlchemey model object
#    extra_fields        : list of Virtual table columns
#    primary_key         : SqlAlchemy column object that should be considered the primary key
#    noupdate            : list of column names that may not be updated
#    noview              : list of column names that may not be returned
#    required            : list of column names that must be set in a create
#    list_columns        : list of column names that are returned in list read
#    sort_columns        : list of column names that are sortable (all if null)
#    memcache_section    : Memcache section name to use for output caching
#    caching_key         : Memcache key name to use for output caching
#    caching_ttl         : Memcache ttl for output caching
#    default_limit       : Default limit of number of records to return
#    max_limit           : Maximum number of records to return
#    parent_keys         : Dictionary of parent column objects,
#                            key name from url/database column name (must match)
#                            value SqlAlchemy column
#    relations           : SqlAlchmeny relations to add to basic query
#    column_objects      : Dictionary of ColumnHelper objects to add to single record output
#    list_column_objects : Dictionary of ColumnHelper objects to add to multi record output
#                          or True if all single record outputs should be returned.
#    allow_bulk_update   : If True then bulk PUT is enabled.
#    extended_bulk_errors : If True extended errors are enabled by default for both bulk POST and bulk PUT.

class ReadObject(object):
    LOCK_TIME = 150

    def __init__(self,
                 sql_alchemy_model=None,
                 extra_fields=dict(),
                 primary_key=None,
                 noupdate=[],
                 noview=[],
                 required=None,
                 list_columns=None,
                 sort_columns=None,
                 memcache_section='api_internal',
                 caching_key=None,
                 caching_ttl=3600,
                 default_limit=0,
                 max_limit=0,
                 parent_keys={},
                 parent_values={},
                 relations=[],
                 column_objects=None,
                 list_column_objects=None,
                 allow_bulk_update=False,
                 extended_bulk_errors=False,
                 bulk_put_single_transaction=False):

        self._sql_alchemy_model = sql_alchemy_model
        virtual_table = self.virtual_table()
        self._extra_fields = collections.OrderedDict(virtual_table if virtual_table else extra_fields)
        self._primary_key = self.get_primary_key(primary_key)

        self._noupdate = noupdate
        self._noview = noview
        self._required = required
        self._list_columns = list_columns
        self._memcache_section = memcache_section
        self._caching_key = caching_key
        self._caching_ttl = caching_ttl
        self._sort_columns = sort_columns
        self._limit_default = default_limit
        self._limit_max = max_limit
        self._parent_keys = parent_keys
        self._parent_values = parent_values
        self._relations = relations
        self._column_objects = column_objects if column_objects else self.column_objects()
        self._list_column_objects = list_column_objects
        self._allow_bulk_update = allow_bulk_update
        self._extended_bulk_errors = extended_bulk_errors
        self._bulk_put_single_transaction = bulk_put_single_transaction

    ###########################################################################
    # Overridable methods
    #

    def automatically_assigned_columns(self):
        """
        This method is used to return a dictonary of key names and values that are always
        assigned when a record is created.
        """
        return {
            'created_at': datetime.datetime.utcnow()
        }

    # First thing called after init of session
    def init(self, db_session, request_params):
        """
        This is the first method called when a request starts, any initialization of the
        scratch pad or other objects can be done here.
        """
        pass

    # Returns primary key sqlalchemy column object
    def get_primary_key(self, parameter):
        """
        This method is used when the CrudObject is constructed and returns the SqlAlchemy column that is the primary key of the table.
        Parameters:
        parameter: The primary_key value from the constructor.
        Return:
        An SqlAlchemy column object.
        """
        if parameter:
            return parameter
        elif self._sql_alchemy_model:
            return class_mapper(self._sql_alchemy_model).primary_key[0]
        else:
            return None

    # Optionally override this to specify a virtual table in a class that extends CrudObject
    def virtual_table(self):
        """
        The virtual_table method is used to return 'virtual column' objects to be used.
        If this returns values it overrides any value passed in the constructor,
        used in the construction of the CrudObject
        """
        return {}

    def column_objects(self):
        """
        This returns a list of column objects to be used in the request, passing a column_objects
        parameter in the constructor will override this method.
        """
        return {}

    # Returns sqlalchemy column object for given name
    def get_column(self, db_session, request_params, name):
        # Note: intended behavior is for extra_fields to clobber the table if there is a naming conflict.
        if name in self._extra_fields.keys():
            return self._extra_fields[name]
        if self._sql_alchemy_model and hasattr(self._sql_alchemy_model, name):
            return getattr(self._sql_alchemy_model, name)
        return None

    # Creates the base query for finding an object
    def base_query(self, db_session, request_params):
        """
        This is one of the more important methods in the class.
        This builds the base query for reading records from the database.
        In some cases where you want override the records being read you can
        override this method to create the query and then override get_values()
        to interpret those values.
        However most of the time you don't need to do that, there are enough other
        things you can override in the query without having to change this.
        """
        if self._sql_alchemy_model:
            return db_session.query(self._sql_alchemy_model, *self._extra_fields.values())
        return db_session.query(*self._extra_fields.values())

    # Returns base query object for list operations
    def base_query_list(self, db_session, request_params):
        """
        This method builds the base query for list requests if the object is setup
        to return a different set of columns than the normal list.
        It can also be overridden as a way to modify the query for a list request.
        """
        if self._list_columns is None:
            return self.base_query(db_session, request_params)
        columns = []
        for i in range(0, len(self._list_columns)):
            columns.append(self.get_column(db_session, request_params, self._list_columns[i]))
        return db_session.query(*columns)

    def add_filter(self, db_session, request_params, query):
        """
        This method is used to add all the additional query values to the base query,
        this includes relations and parent keys defined in the CrudObject constructor.
        It is often useful to override this method to add additional filters to your query.
        """
        for key, column in self.get_parent_keys(db_session, request_params).items():
            query = query.filter(column == request_params.arguments[key])
        for relation in self.get_relations(db_session, request_params):
            if isinstance(relation, (list, tuple)) and len(relation) == 2:
                query = query.outerjoin(relation[0], relation[1])
            else:
                query = query.filter(relation)
        for key, column in self._parent_values.items():
            query = query.filter(column == getattr(self._sql_alchemy_model, key))
        return query

    # Returns the list of parent key relations as dictionary
    def get_parent_keys(self, db_session, request_params):
        """
        This returns the dictionary of parent key names/column objects for the request.
        Normally you don't need to override this.
        """
        return self._parent_keys

    # Return list of normal relations as list

    # Note when defining the 'releations', you can give a two element tuple or array
    # Where first element is a table and the second is a releation, then it will do
    # an outer join.

    def get_relations(self, db_session, request_params):
        """
        this returns the 'relations' as defined in the constructor, this provides a place to
        define them in the class rather than in the constructor, the format is the same as the constructor.
        """
        return self._relations

    # Applies primary key query
    def filter_primary(self, db_session, request_params, query, resource_id):
        """
        This applies a filter by the primary key to the query.
        You probably won't need to override this, nobody has so far.
        """
        return query.filter(self._primary_key == resource_id)

    # Convers raw query result into dictionary
    def get_values(self, db_session, request_params, raw_result):
        """
        This is one of the more important methods in the class, this takes the result of one iteration of the built query and converts it into a dictionary to be returned
        Parameters:
        db_session
        request_params
        raw_result - This is one result/iteration of the query that was constructed.
        Return:
        A dictionary of values.
        """
        if self._sql_alchemy_model and not self._extra_fields:
            # session.query(Table)
            return as_dict(raw_result, self._noview)
        elif self._sql_alchemy_model:
            # session.query(Table,*extra_fields)
            values = as_dict(raw_result[0], self._noview)
            # Note: intended behavior is for extra_fields to clobber the table if there is a naming conflict.
            for idx, name in enumerate(self._extra_fields.keys()):
                values[name] = raw_result[idx + 1]
            return values
        else:
            # session.query(*extra_fields)
            values = {}
            for idx, name in enumerate(self._extra_fields.keys()):
                values[name] = raw_result[idx]
            return values

    def get_column_object_values(self, db_session, request_params, raw_result, values, column_dict):
        """
        This method is used to get the values for the column objects.  You should not need to
        override this, nobody has so far.
        """
        if column_dict is not None:
            do_filter = request_params.parameters.get('f', None)
            columns = None
            if not do_filter is None:
                columns = do_filter.split(',')
            for key, obj in column_dict.items():
                if columns is None or key in columns:
                    values[key] = obj.get(db_session,
                                          raw_result[0] if isinstance(raw_result, (list, tuple)) else raw_result, key)

    # Called after get_values for single column reads
    def get_values_extra(self, db_session, request_params, raw_result, values):
        """
        This is a wrapper for get_column_object_values that is just for the default column objects.
        You don't normally need to override this but if there are values you need to add that are
        needed in a list get that are not provided by the other methods it can be done here.
        """
        self.get_column_object_values(db_session, request_params, raw_result, values, self._column_objects)
        return values

    # Convers raw query result into dictionary for list searches
    def get_values_list(self, db_session, request_params, raw_result):
        """
        This is the get_values method when doing a list type read from the object.  If list_columns are not
        defined in the constructor it will call the base get_values() method else it will do it itself.
        So far this method has not been overridden and there is probably little reason to do so.
        """
        if self._list_columns is None:
            values = self.get_values(db_session, request_params, raw_result)
        else:
            values = {}
            for i in range(0, len(self._list_columns)):
                values[self._list_columns[i]] = raw_result[i];
        if self._list_column_objects == True:
            self.get_column_object_values(db_session, request_params, raw_result, values, self._column_objects)
        elif isinstance(self._list_column_objects, dict):
            self.get_column_object_values(db_session, request_params, raw_result, values, self._list_column_objects)
        return values

    def get_limit(self, db_session, request_params, limit_get):
        """
        This returns the limit to apply to the query, or return zero if there is no limit
        """
        if limit_get is None or limit_get < 0:
            if self._limit_default == 0:
                return self._limit_max
            return self._limit_default
        if self._limit_max < 1:
            return limit_get
        return min(self._limit_max, limit_get)

    # Applies column filtesr to output
    def filter_values(self, db_session, request_params, values):
        """
        If a 'f' parameter was given as a query parameter, this is the method that will reduce the values
        data down to just the columns specified.  There is little reason to override this.
        """
        do_filter = request_params.parameters.get('f', None)
        if not do_filter is None:
            new_val = {}
            for c in do_filter.split(','):
                if values.has_key(c):
                    new_val[c] = values[c]
                else:
                    raise CrudResponsePlain('Invalid filter column {}'.format(c), 400)
            return new_val
        return values

    # Generates return headers
    def response_headers(self, db_session, request_params):
        """
        This returns the headers to be returned for the current request.
        """
        headers = {}
        if request_params.cache_level:
            headers[CACHE_LEVEL_NAME] = request_params.cache_level
        return headers

    # Applys a specific filter to the query
    def apply_filter(self, db_session, request_params, query, column, op, value):
        """
        This method is used to apply a filter to the base query, it receives each of the filters applied in the 'q' parameter, calling the apply_filter function from the db_helpers module.
        Parameters:
        db_session - an SQLalchemy database session
        request_param - RouteRequestParams object for the request
        query - The query being built for the request
        column - The name of the column to filter
        op - the filter op to apply ('eq', 'le', 'ne',,,,)
        value - The value being specified for the filter
        Return:
        The updated query object.
        """
        col = self.get_column(db_session, request_params, column)
        if col is None:
            raise CrudResponsePlain('Invalid filter column {}'.format(column), 400)
        return apply_filter(query, col, op, value)

    # Applies text search filters to the query
    def apply_text_filter(self, db_session, request_params, query, value):
        """
        This applies the filter when a text or 't' type query parameter is given,
        it is supposed to apply a like type filter where the value could be anywhere in the column,
        it does this by surrounding the value with % characters in the query.  The data must
        have been stored in the 'TextSearch' table linked to the primary key of the object.
        This may be obsolete.
        """
        query = query.filter(TextSearch.table_text.like('%{}%'.format(value))) \
            .filter(TextSearch.table_name == self._sql_alchemy_model.__tablename__) \
            .filter(TextSearch.table_id == self._primary_key)

    # Applies text search match filters to the query
    def apply_match_filter(self, db_session, request_params, query, value):
        """
        This applies the filter when a matches or 'm' type query parameter is given,
        it is supposed to apply a match type filter where the value could be anywhere in the column,
        The data must have been stored in the 'TextSearch' table linked to the primary key of the object.
        This may be obsolete.
        """
        query = query.filter(TextSearch.table_text.match(value)) \
            .filter(TextSearch.table_name == self._sql_alchemy_model.__tablename__) \
            .filter(TextSearch.table_id == self._primary_key)

    # Operates on a get parameter
    def apply_parameter(self, db_session, request_params, query, param, value):
        """
        This method applies HTTP query parameters to the query, all parameters except limit (l) and offset (o) are applied here.  Calling the appropriate method to apply the filter.
        Parameters:
            db_session - an SQLalchemy database session
            request_params - RouteRequestParams object for the request
            query - the query object being built.
            param - the name HTTP query parameter being applied.
            value - the value of the HTTP query parameter being applied.
        Return
        the query object be created.
        """
        if param == 'q':
            for name, ops in json.loads(value).items():
                for op, value in ops.items():
                    if name == 'is_deleted':
                        is_deleted = False
                    query = self.apply_filter(db_session, request_params, query, name, op, value)
        elif param == 't':
            query = self.apply_text_filter(db_session, request_params, query, value)
        elif param == 'm':
            query = self.apply_match_filter(db_session, request_params, query, value)
        else:
            query = self.apply_filter(db_session, request_params, query, param, 'eq', value)
        return query

    # Applies a sorting operation on the query
    def apply_sorting(self, db_session, request_params, query, name, reverse):
        """
        This method applies the sorting for a specific column to the query.  The 'sorting' method is called first to interpret the 's' parameter, but this method is called to do the actual work.
        Parameters:
            db_session - an SQLalchemy database session
            request_param - RouteRequestParams object for the request
            query
            name - The name of the column to be sorted.
            reverse - True if the sort should be in reverse order.
        Return
        The modified query
        """
        col = self.get_column(db_session, request_params, name)
        if col is None:
            raise CrudResponsePlain("Invalid sort column {}".format(name), 400)
        if reverse:
            query = query.order_by(col.desc())
        else:
            query = query.order_by(col)
        return query

    def sorting(self, db_session, request_params, query, sort_by_criteria):
        """
        This method interprets the values of the sorting 's' parameters.  It checks that the column is a sortable column and then calls the apply_sorting to apply the sort to the query.
        Parameters
            db_session - an SQLalchemy database session
            request_params - RouteRequestParams object for the request
            query
            sort_by_criteria - the value of the 's' query parameter that has already been split on comma.
        Return
        The updated query
        """
        for name in sort_by_criteria:
            if self._sort_columns is None or self._sort_columns.has_key(name):
                reverse = False
                if name.startswith('-'):
                    reverse = True
                    name = name[1:]
                elif name.endswith('-'):
                    name = name[:len(name) - 1]
                    reverse = True
                elif name.startswith('+'):
                    name = name[1:]
                elif name.endswith('+'):
                    name = name[:len(name) - 1]
                query = self.apply_sorting(db_session, request_params, query, name, reverse)
            else:
                raise CrudResponsePlain("Invalid sort column {}".name(name))

        return query

    # Applies the default sorting to the query
    def default_sorting(self, db_session, request_params, query):
        """
        This method is called to apply sorting to the query if no sorting parameter is in the GET request.
        Parameters:
            db_session - an SQLalchemy database session
            request_params - RouteRequestParams object for the request
            query
        Return
        The updated query.
        """
        return query

    # Final update to return payload for list.
    def get_all_finish(self, db_session, request_params, payload):
        """
        This method is called during a list type get request after the response has been built so that additional values can be added or any final processing can be done.   It receives the full payload to be returned before its converted to json.  The caller can add additional values but should not add additional values to the result array as the limit/offset values will not make any sense anymore.
        Parameters:
            db_session - an SQLalchemy database session
            request_params - RouteRequestParams object for the request
            payload - The payload that is about to be returned to the caller.
        Return
        The modified payload.
        """
        pass

    # These methods are called before the DB read to make sure the request is valid.
    def is_valid_get(self, db_session, request_params, resource_id=None):
        """
        Checks to see if the GET request is valid.  Unlike the others if the request is invalid it must raise an exception.
        Parameters:
            db_session - an SQLalchemy database session
            request_params - RouteRequestParams object for the request
            resource_id - The ID of the record being read or None if it is a list type request.
        Return:
        True or raise an Exception.
        """
        return self.is_valid(db_session, request_params)

    def is_valid_post(self, db_session, request_params, resource_dict):
        """
        This is used to validate a create/post request.  If not defined it will call the is_valid_write() method.  if not defined it will call is_valid_write.
        Parameters:
            db_session - an SQLalchemy database session
            request_params - RouteRequestParams object for the request
            resource_dict - The dictionary of values in the post.
        Return:
        True or False or raise an exception.
        """
        return self.is_valid_write(db_session, request_params)

    def is_valid_put(self, db_session, request_params, resource_id, resource_dict):
        """
        This is used to validate a write/pUt request.  If not defined it will call the is_valid_write() method.  if not defined it will call is_valid_write.
        Parameters:
            db_session - an SQLalchemy database session
            request_params - RouteRequestParams object for the request
            resource_id - The ID of the record being updated.
            resource_dict - The dictionary of values in the post.
        Return:
        True or False or raise an exception.
        """
        return self.is_valid_write(db_session, request_params)

    def is_valid_del(self, db_session, request_params, resource_id):
        """
        This checks to see if it is a valid DELETE request.  if not defined it will call is_valid_write.
        Parameters:
            db_session - an SQLalchemy database session
            request_params - RouteRequestParams object for the request
            resource_id - The ID of the record being updated.
        Return:
        True or False or raise an exception.
        """
        return self.is_valid_write(db_session, request_params)

    def is_valid_write(self, db_session, request_params):
        """
        This is a general request that can be used to check all writes as it is the default if the other write tests are not defined.  If not defined will return False for the ReadObject or call is_valid for the CrudObject.
        Parameters:
            db_session - an SQLalchemy database session
            request_params - RouteRequestParams object for the request
        Return
        True or False.
        """
        return False

    def is_valid(self, db_session, request_params):
        """
        This is a general catch all if no other is_valid methods are defined.
        Parameters
            db_session - an SQLalchemy database session
            request_params - RouteRequestParams object for the request
        Return
        True, False or raise Exception.
        """
        return True

    def is_valid_lock(self, db_session, request_params, db_record, user_id):
        """
        This checks if the caller is able to create a lock for the resource.
        Parameters
            db_session - an SQLalchemy database session
            request_params - RouteRequestParams object for the request
            db_record - The record to be locked
            user_id - The user_id number of the caller.
        Return
        True, False or raise exception.
        """
        return True

    ###########################################################################
    # Core methods
    #

    # Over-rideable Returns the ttl and memcache key to use for this find
    # Return -1, None if no caching should be done.
    def get_cache_key(self, db_session, request_params, raw_result, list_mode):
        return -1, None

    def memcache_session(self, db_session, request_params):
        return request_params.memcache_session(self._memcache_section)

    def get_values_cached(self, db_session, request_params, raw_result, list_mode):
        ttl = -1
        mc_key = None
        if request_params.headers.get('X-Linqia-No-Cache', 'false').lower() != 'true':
            ttl, mc_key = self.get_cache_key(db_session, request_params, raw_result, list_mode)
        if not mc_key is None and ttl != -1:
            memcache = self.memcache_session(db_session, request_params)
            rawblob = memcache.get(mc_key)
            if not rawblob is None:
                values = json.loads(rawblob)
                values['zzz_memache_read'] = True
                request_params.cache_level |= CACHE_LEVEL_OVERLAY_HIT
                return values
        if list_mode:
            values = self.get_values_list(db_session, request_params, raw_result)
        else:
            values = self.get_values(db_session, request_params, raw_result)
            values = self.get_values_extra(db_session, request_params, raw_result, values)
        if not mc_key is None and ttl != -1:
            blob = json.dumps(values, cls=LinqiaJsonEncoder)
            memcache.set(mc_key, blob, ttl)
            request_params.cache_level |= CACHE_LEVEL_OVERLAY_MISS
            values['zzz_memache_read'] = False
        return values

    def clear_values_cached(self, db_session, request_params, raw_result):
        mc_key = None
        ttl, mc_key = self.get_cache_key(db_session, request_params, raw_result, False)
        if not mc_key is None and ttl != -1:
            memcache = self.memcache_session(db_session, request_params)
            if memcache.exists(mc_key):
                memcache.delete(mc_key)

    # Page cache key (Page caching is not yet implemented)
    # Returns the base cache key to be used for page caching.
    # Additional values for sorting, filtering, limits and offset
    # will be added to the base key before its actually used
    def page_cache_key(self, db_session, request_params, cache_key):
        if self._caching_key is None or cache_key is None:
            return None
        return "{}_{}".format(self._caching_key, cache_key)

    # TODO
    def get_page_key(self, db_session, request_params, cache_key):
        return None

    # TODO
    def get_page_cache(self, db_session, request_params, page_key):
        return None

    # TODO
    def set_page_cache(self, db_session, request_params, page_key, payload):
        return None

    ###########################################################################
    # Core methods
    #

    def get_one(self, db_engine, request_params, resource_id):
        """
        This method  it the main top level method that is called when a single record is requested and is called directly by the bottle route method.  In general its best not to override this method as its very core to the system.
        Parameters:
            db_engine - The database engine to use for the request, this method will create the session for the other methods.
            request_params - RouteRequestParams object for the request
            resource_id - The ID of the requested object.
        Return
        None, it will raise either a CrudResponseJson or a CrudResponsePlain depending on the result of the query.
        """
        try:
            with session_scope(db_engine) as session:
                self.init(session, request_params)
                self.is_valid_get(session, request_params, resource_id)
                query = self.base_query(session, request_params)
                query = self.add_filter(session, request_params, query)
                query = self.filter_primary(session, request_params, query, resource_id)
                raw_result = query.first()
                if raw_result is None:
                    raise CrudResponsePlain("Not found", 404)

                v1 = self.get_values_cached(session, request_params, raw_result, False)
                v2 = self.filter_values(session, request_params, v1)

                return CrudResponseJson(v2, headers=self.response_headers(session, request_params))
        except (SQLAlchemyError) as e:
            return CrudResponsePlain(str(e), 500)

    def get_all(self, db_engine, request_params):
        """
        This method  it the main top level method that is called when a list of records is requested and is called directly by the bottle route method.  In general its best not to override this method as its very core to the system.
        Parameters:
            db_engine - The database engine to use for the request, this method will create the session for the other methods.
            request_params - RouteRequestParams object for the request
        Return
        None, it will raise either a CrudResponseJson or a CrudResponsePlain depending on the result of the query.
        """
        try:
            with session_scope(db_engine) as session:
                self.init(session, request_params)
                self.is_valid_get(session, request_params)
                cache_key = request_params.parameters.get('cachekey')
                if cache_key is None or cache_key.lower() == 'new':
                    cache_key = binascii.hexlify(os.urandom(6))
                cache_key = self.page_cache_key(session, request_params, cache_key)
                # TODO Page caching get
                query = self.base_query_list(session, request_params)
                query = self.add_filter(session, request_params, query)
                limit = self._limit_default
                offset = 0
                request_params.is_deleted = not self._sql_alchemy_model is None and hasattr(self._sql_alchemy_model,
                                                                                            'is_deleted')
                request_params.is_archived = not self._sql_alchemy_model is None and hasattr(self._sql_alchemy_model,
                                                                                             'is_archived')
                sort_by_criteria = None
                for param, value in request_params.parameters.items():
                    if param == 'l':
                        limit = self.get_limit(session, request_params, int(value))
                    elif param == 'o':
                        offset = int(value)
                    elif param == 's':
                        sort_by_criteria = value.split(',')
                    elif param != 'f' and param != 'cachekey':
                        query = self.apply_parameter(session, request_params, query, param, value)

                if request_params.is_deleted:
                    query = query.filter(self._sql_alchemy_model.is_deleted == False)
                if request_params.is_archived:
                    query = query.filter(self._sql_alchemy_model.is_archived == False)
                count = query.count()
                if sort_by_criteria:
                    query = self.sorting(session, request_params, query, sort_by_criteria)
                else:
                    query = self.default_sorting(session, request_params, query)

                if limit > 0:
                    query = query.limit(limit)
                if offset > 0:
                    query = query.offset(offset)
                payload = {'c': count, 'l': limit, 'o': offset, 'results': []}
                for raw_result in query:
                    v1 = self.get_values_cached(session, request_params, raw_result, True)
                    v2 = self.filter_values(session, request_params, v1)
                    payload['results'].append(v2)
                headers = None
                self.get_all_finish(session, request_params, payload)
                # TODO page caching set
            return self.get_response(session, request_params, payload)
        except (SQLAlchemyError) as e:
            return CrudResponsePlain(str(e), 500)

    def get_response(self, db_session, request_params, payload):
        return CrudResponseJson(payload, headers=self.response_headers(db_session, request_params))

    def create(self, db_engine, request_params, resource_dict):
        raise CrudResponsePlain("Not implemented", 405)

    def update(self, db_engine, request_params, resource_id, resource_dict):
        raise CrudResponsePlain("Not implemented", 405)

    def delete(self, db_engine, request_params, resource_id):
        raise CrudResponsePlain("Not implemented", 405)


class CrudObject(ReadObject):

    ###########################################################################
    # Authorization methods
    #

    def write_restricted_cols(self):
        return {'updated_at', 'created_at'}

    def is_valid_write(self, db_session, request_params):
        if self.is_valid(db_session, request_params):
            if self._sql_alchemy_model is None:
                raise CrudResponsePlain("Update attempted where there is no model", 500)
            return True
        return False

    def is_valid_put(self, db_session, request_params, resource_id, resource_dict):
        self.is_valid_update_lock(db_session, request_params, resource_id, resource_dict)
        return super(CrudObject, self).is_valid_put(db_session, request_params, resource_id, resource_dict)

    # Returns true if the primary key can be modified
    def update_primary_key(self, db_session, request_params, db_record, key, value):
        return False

    ###########################################################################
    # Methods called before a change is commiteed to the database
    #
    # The (on) methods are used during updates requests and the are called before the
    # commit is performed.  Generally they are for assigning additional values or
    # records before the commit is performed.  They may call commit() on the db_session
    # if needed, but it is not recommended as if an exception is performed later records
    # will have been written to database and not rolled back.  Notifications and other
    # things that need to be done after the transaction should be in the (after) methods.

    def on_create(self, db_session, request_params, resource_dict, db_record):
        automatically_assigned_columns = self.automatically_assigned_columns()
        for col, value in automatically_assigned_columns.iteritems():
            if hasattr(self._sql_alchemy_model, col):
                setattr(db_record, col, value)
        for key, column in self.get_parent_keys(db_session, request_params).items():
            if column.name != 'id' and hasattr(self._sql_alchemy_model, column.name):
                setattr(db_record, column.name, request_params.arguments[key])

    def on_update(self, db_session, request_params, resource_dict, db_record):
        pass

    def on_delete(self, db_session, request_params, db_record):
        pass

    ###########################################################################
    # Methods called after a change is commiteed to the database
    #
    # The after methods are called during update requests but are called after the
    # commit is performed and the values committed to the database.  While yon can do
    # additional writes to the database you should avoid throwing any exceptions as
    # you will override the response going to the caller in such a way they may think
    # the values have not been written to the database when in fact they have.

    def after_create(self, db_session, request_params, resource_dict, db_record):
        pass

    def after_update(self, db_session, request_params, resource_dict, db_record):
        pass

    def after_delete(self, db_session, request_params, db_record):
        pass

    ###########################################################################
    # Overrideable methods
    #

    def get_create_row(self, db_session, request_params, resource_dict):
        return self._sql_alchemy_model()

    def get_update_row(self, db_session, request_params, raw_result):
        if isinstance(raw_result, list) or isinstance(raw_result, tuple):
            return raw_result[0]
        return raw_result

    # Returns the value of the primary key from the record.
    def primary_key_value(self, db_session, request_params, db_record):
        pk = self._primary_key
        return getattr(db_record, pk.name)

    # Throw exception if not valid, return True if changed, False if not
    def update_column(self, db_session, request_params, db_record, key, value, is_create):
        """
        This applies an update of one column to the record that is being updated/created.
        IT returns True if the update was applied or False if it was not.  False does not
        mean an error, just that the value was not updated for some reason, this could
        include that the new value was the same as the old.
        Parameters:
            db_sessino
            request_params
            db_record - The record being updated
            key - The name of the column being updated
            value - The value to apply
            is_create - True if its a create, False if an update
        Return
        True if the value was applied, False if not.
        """
        if not is_create and hasattr(db_record, key) and getattr(db_record, key) == value:
            # No change, ignore
            return False

        if self._extra_fields.has_key(key):
            raise CrudResponsePlain("Column {} is not updateable".format(key), 400)

        if not self._noupdate is None:
            if key in self._noupdate:
                raise CrudResponsePlain("Column {} is not updateable".format(key), 400)

        if self._parent_values.has_key(key):
            raise CrudResponsePlain("Parent column {} is not updateable".format(key), 400)

        if key in self.write_restricted_cols():
            if is_create:
                raise CrudResponsePlain("Column {} is not updateable".format(key), 400)
            # In updates just silently ignore
            return False

        if self.get_parent_keys(db_session, request_params).has_key(key):
            if request_params.arguments[key] != value:
                CrudResponsePlain("Column {} parent key mismatch".format(key), 400)
            return False

        if hasattr(self._sql_alchemy_model, key):
            mod_col = getattr(self._sql_alchemy_model, key)
        else:
            raise CrudResponsePlain("Unknown Column {}".format(key), 400)
        pk = self._primary_key
        if mod_col == pk:
            if is_create:
                if not self.update_primary_key(db_session, request_params, db_record, key, value):
                    raise CrudResponsePlain("Primary key {} is not updateable".format(key), 400)
            else:
                if value != getattr(db_record, pk.name):
                    raise CrudResponsePlain("Primary key {} is not updateable".format(key), 400)
        setattr(db_record, key, value)
        return True

    # Throw exception if required column is missing.
    def required_columns(self, db_session, request_params, resource_dict):
        if not self._required is None:
            for key in self._required:
                if not resource_dict.has_key(key):
                    raise CrudResponsePlain("Column {} is missing".format(key), 400)

    # Split out so we can perform soft deletes
    def perform_delete(self, db_session, request_params, db_record):
        db_session.delete(db_record)

    def create_commit(self, db_session, request_params, db_record):
        db_session.add(db_record)
        db_session.flush()
        db_session.commit()

    ###########################################################################
    # Core methods
    #

    def _update_columns(self, db_session, request_params, resource_dict, db_record, is_create):
        updated = False
        if self._parent_values and is_create:
            q = db_session.query(*self._parent_values.values())
            for key, column in self.get_parent_keys(db_session, request_params).items():
                if column.class_ is not self._sql_alchemy_model:
                    q = q.filter(column == request_params.arguments[key])
            for key, value in zip(self._parent_values.keys(), q.first()):
                updated = True
                setattr(db_record, key, value)

        for key, value in resource_dict.items():
            if self._column_objects.has_key(key):
                self._column_objects[key].test_valid(db_session, db_record, key, value)
            else:
                if self.update_column(db_session, request_params, db_record, key, value, is_create):
                    updated = True
        return updated

    def _update_columns_extra(self, db_session, request_params, resource_dict, db_record, is_create):
        updated = False
        for key, column in self._column_objects.items():
            if resource_dict.has_key(key):
                column.set(db_session, db_record, key, resource_dict[key])
                updated = True

        return updated

    def create(self, db_engine, request_params, resource_dict):
        """
        This method  it the main top level method that is called when a record is created
        and is called directly by the bottle route method.  In general its best not to override
        this method as its very core to the system.
        Parameters
            db_engine - The database engine to use for the request, this method will create the session for the other methods.
            request_params
            resource_dict
        Return
        None, raises a CrudResonseExceptin
        """
        try:
            with session_scope(db_engine) as session:
                self.init(session, request_params)
                if isinstance(resource_dict, (list, tuple)):
                    if self._extended_bulk_errors:
                        rows = {}
                        response = []
                        for row in resource_dict:
                            if not self.is_valid_post(session, request_params, row):
                                response.append(dict(code=400, body="Invalid Create", data=row))
                                continue
                            try:
                                self.required_columns(session, request_params, row)
                                model = self.get_create_row(session, request_params, row)
                                self._update_columns(session, request_params, row, model, True)
                                self.on_create(session, request_params, row, model)
                                self.create_commit(session, request_params, model)
                                rows[model] = row
                            except CrudResponseException as e:
                                response.append(dict(code=e.getStatusCode(), body=e.getBody(), data=row))
                            except (SQLAlchemyError) as e:
                                session.rollback()
                                response.append(dict(code=400, body=str(e), data=row))
                            except Exception as e:
                                response.append(dict(code=400, body=str(e), data=row))
                        for model, row in rows.items():
                            self._update_columns_extra(session, request_params, row, model, True)
                            self.after_create(session, request_params, row, model)
                            pkv = self.primary_key_value(session, request_params, model)
                            response.append(dict(code=201, body="{}/{}".format(request_params.request_uri, pkv)))

                    else:
                        for row in resource_dict:
                            if not self.is_valid_post(session, request_params, row):
                                raise CrudResponsePlain("Invalid Create", 400)
                            self.required_columns(session, request_params, row)
                        rows = {}
                        response = []
                        for row in resource_dict:
                            model = self.get_create_row(session, request_params, row)
                            self._update_columns(session, request_params, row, model, True)
                            self.on_create(session, request_params, row, model)
                            self.create_commit(session, request_params, model)
                            rows[model] = row
                        for model, row in rows.items():
                            self._update_columns_extra(session, request_params, row, model, True)
                            self.after_create(session, request_params, row, model)
                            pkv = self.primary_key_value(session, request_params, model)
                            response.append("{}/{}".format(request_params.request_uri, pkv))
                    return CrudResponseJson(response, 201)
                else:
                    if not self.is_valid_post(session, request_params, resource_dict):
                        raise CrudResponsePlain("Invalid Create", 400)
                    model = self.get_create_row(session, request_params, resource_dict)
                    self.required_columns(session, request_params, resource_dict)
                    self._update_columns(session, request_params, resource_dict, model, True)
                    self.on_create(session, request_params, resource_dict, model)
                    self.create_commit(session, request_params, model)
                    self._update_columns_extra(session, request_params, resource_dict, model, True)
                    self.after_create(session, request_params, resource_dict, model)
                    pkv = self.primary_key_value(session, request_params, model)
                    return CrudResponsePlain("{}/{}".format(request_params.request_uri, pkv), 201)

        except (SQLAlchemyError) as e:
            return CrudResponsePlain(str(e), 400)

    # Actual update logic split out so that it can be called separate from the normal framework
    # that returns the payload.
    def update_core(self, db_session, request_params, resource_id, resource_dict):
        """
        This is the core of the code that updates a record, it is separated from the top level
        update() method so that it can be called by the bulk put method or in other situations
        where its required.
        Parameters:
            db_session
            request_params
            resource_id
            resource_dict
        Return
        True if an update was performed, may be False if not columns were changed.
        """
        query = self.base_query(db_session, request_params)
        query = self.add_filter(db_session, request_params, query)
        query = self.filter_primary(db_session, request_params, query, resource_id)
        raw_result = query.first()
        if raw_result is None:
            raise CrudResponsePlain("Not found", 404)
        model = self.get_update_row(db_session, request_params, raw_result)
        updated = self._update_columns(db_session, request_params, resource_dict, model, False)
        if updated:
            self.on_update(db_session, request_params, resource_dict, model)
            db_session.add(model)
            db_session.commit()
        up2 = self._update_columns_extra(db_session, request_params, resource_dict, model, False)
        self.retainlock(db_session, request_params, resource_dict, model)
        if updated or up2:
            self.after_update(db_session, request_params, resource_dict, model)
            self.clear_values_cached(db_session, request_params, raw_result)
            return True

    def update_core_single_transaction(self, db_session, request_params, resource_id, resource_dict):
        query = self.base_query(db_session, request_params)
        query = self.add_filter(db_session, request_params, query)
        query = self.filter_primary(db_session, request_params, query, resource_id)
        raw_result = query.first()
        if raw_result is None:
            raise CrudResponsePlain("Not found", 404)
        model = self.get_update_row(db_session, request_params, raw_result)
        updated = self._update_columns(db_session, request_params, resource_dict, model, False)
        if updated:
            self.on_update(db_session, request_params, resource_dict, model)
            db_session.add(model)
            db_session.flush()
        up2 = self._update_columns_extra(db_session, request_params, resource_dict, model, False)
        self.retainlock(db_session, request_params, resource_dict, model)
        if updated or up2:
            self.after_update(db_session, request_params, resource_dict, model)
            self.clear_values_cached(db_session, request_params, raw_result)
            return True

    def do_finally(self, db_session, request_params, resource_id, resource_dict):
        '''
        This method is to be executed after the database commit on a single transaction bulk put.
        '''
        pass

    def update(self, db_engine, request_params, resource_id, resource_dict):
        """
        This method  it the main top level method that is called when a record is
        updated. and is called directly by the bottle route method.  In general its
        best not to override this method as its very core to the system.
        Parameters
            db_engine - The database engine to use for the request, this method will create the session for the other methods.
            request_params
            resource_id
            resource_dict
        Return
        None, raises a CrudResonseExceptin
        """
        try:
            with session_scope(db_engine) as session:
                self.init(session, request_params)
                if not self.is_valid_put(session, request_params, resource_id, resource_dict):
                    raise CrudResponsePlain("Invalid Update", 400)
                self.update_core(session, request_params, resource_id, resource_dict)
            return self.get_one(db_engine, request_params, resource_id)
        except (SQLAlchemyError) as e:
            return CrudResponsePlain(str(e), 400)

    def delete(self, db_engine, request_params, resource_id):
        """
        This method  it the main top level method that is called when a record is deleted. and is called directly by the bottle route method.  In general its best not to override this method as its very core to the system.
        Parameters
            db_engine - The database engine to use for the request, this method will create the session for the other methods.
            request_params
            resource_id
            resource_dict
        Return
        None, raises a CrudResonseExceptin
        """
        try:
            with session_scope(db_engine) as session:
                self.init(session, request_params)
                if not self.is_valid_del(session, request_params, resource_id):
                    raise CrudResponsePlain("Invalid Delete", 400)
                query = self.base_query(session, request_params)
                query = self.add_filter(session, request_params, query)
                query = self.filter_primary(session, request_params, query, resource_id)
                raw_result = query.first()
                if raw_result is None:
                    raise CrudResponsePlain("Not found", 404)
                model = self.get_update_row(session, request_params, raw_result)
                self.on_delete(session, request_params, model)
                self.perform_delete(session, request_params, model)
                session.commit()
                self.after_delete(session, request_params, model)
            return CrudResponsePlain("Deleted resource {}".format(resource_id, 200))
        except (SQLAlchemyError) as e:
            return CrudResponsePlain(str(e), 400)

    def delete_single_transaction(self, db_engine, request_params, resource_id):
        try:
            with session_scope(db_engine) as session:
                self.init(session, request_params)
                if not self.is_valid_del(session, request_params, resource_id):
                    raise CrudResponsePlain("Invalid Delete", 400)
                query = self.base_query(session, request_params)
                query = self.add_filter(session, request_params, query)
                query = self.filter_primary(session, request_params, query, resource_id)
                raw_result = query.first()
                if raw_result is None:
                    raise CrudResponsePlain("Not found", 404)
                model = self.get_update_row(session, request_params, raw_result)
                self.on_delete(session, request_params, model)
                self.perform_delete(session, request_params, model)
                session.flush()
                self.after_delete(session, request_params, model)
            return CrudResponsePlain("Deleted resource {}".format(resource_id, 200))
        except (SQLAlchemyError) as e:
            return CrudResponsePlain(str(e), 400)

    def put(self, db_engine, request_params, resource_dict):
        """
        This method  it the main top level method that is called when a bulk put is
        performed and is called directly by the bottle route method. Initially we did
        not have a general method for doing bulk puts and callers would override this
        for there own requirements but now its best to use the standard method.
        Parameters
            db_engine - The database engine to use for the request, this method will create the session for the other methods.
            request_params
            resource_dict
        Return
        None, raises a CrudResonseExceptin
        """
        if not self._allow_bulk_update:
            raise CrudResponsePlain("Bulk Update not permitted", 405)
        try:
            response = []
            if self._extended_bulk_errors:
                for row in resource_dict:
                    try:
                        if row.get('delete') == True:
                            resp = self.delete(db_engine, request_params, row[self._primary_key.name])
                        else:
                            resp = self.update(db_engine, request_params, row[self._primary_key.name], row)
                        if isinstance(resp, CrudResponseJson):
                            response.append(dict(code=200, body=resp.json))
                        else:
                            response.append(dict(code=200, body=resp.getBody()))
                    except CrudResponseException as e:
                        response.append(dict(code=e.getStatusCode(), body=e.getBody(), data=row))
                    except Exception as e:
                        response.append(dict(code=400, body=str(e), data=row))
            else:
                for row in resource_dict:
                    resp = self.update(db_engine, request_params, row['id'], row)
                    if isinstance(resp, CrudResponseJson):
                        response.append(resp.json)
                    else:
                        response.append(resp)
            return CrudResponseJson(response, 200)
        except SQLAlchemyError as e:
            logging.error(str(e))
            return CrudResponsePlain("Error in bulk update", 400)

    def put_single_transaction(self, db_engine, request_params, resource_dict):
        if not self._allow_bulk_update:
            raise CrudResponsePlain("Bulk Update not permitted", 405)
        try:
            response = []

            if not isinstance(resource_dict, list):
                resource_dict = [resource_dict]
            with session_scope(db_engine) as session:
                for row in resource_dict:
                    try:
                        if row.get('delete') == True:
                            resp = self.delete_single_transaction(db_engine, request_params,
                                                                  row[self._primary_key.name])
                        else:
                            updated = self.update_core_single_transaction(session, request_params,
                                                                          row[self._primary_key.name], row)
                    except CrudResponseException as e:
                        response.append(dict(code=e.getStatusCode(), body=e.getBody(), data=row))
                    except Exception as e:
                        response.append(dict(code=400, body=str(e), data=row))
                session.commit()
                self.do_finally(session, request_params, row[self._primary_key.name], resource_dict)
                for row in resource_dict:
                    resp = self.get_one(db_engine, request_params, row[self._primary_key.name])
                    if isinstance(resp, CrudResponseJson):
                        response.append(dict(code=200, body=resp.json))
                    else:
                        response.append(dict(code=200, body=resp.getBody()))
            return CrudResponseJson(response, 200)
        except (SQLAlchemyError) as e:
            return CrudResponsePlain(str(e), 400)

    ###########################################################################
    # Lock methods.
    #
    def is_valid_update_lock(self, db_session, request_params, resource_id, resource_dict):
        if LOCK in request_params.parameters:
            reef_user_id = request_params.headers.get(REEF_USER_ID)
            lock_id = int(request_params.parameters[LOCK])
            try:
                lock = self._lock_get_resource(db_session, request_params, lock_id, reef_user_id)
            except Exception as e:
                raise CrudResponsePlain("Invalid lock value '{}'".format(lock_id), 409)

            if lock is None or lock.id != lock_id:
                raise CrudResponsePlain("Invalid lock value '{}'".format(lock_id), 409)
            if RETAINLOCK in request_params.parameters:
                retainlock = request_params.parameters[RETAINLOCK]
                if not retainlock.lower() in ('true', 'false'):
                    raise CrudResponsePlain("Invalid retainlock value '{}'".format(retainlock), 400)

    def retainlock(self, db_session, request_params, resource_dict, db_record):
        if LOCK in request_params.parameters:
            if RETAINLOCK in request_params.parameters:
                retainlock = request_params.parameters[RETAINLOCK]
                if retainlock.lower() == 'true':
                    return
            reef_user_id = request_params.headers.get(REEF_USER_ID)
            lock_id = int(request_params.parameters[LOCK])
            lock = self._lock_get_resource(db_session, request_params, lock_id, reef_user_id)
            if lock is not None:
                db_session.delete(lock)

    def _lock_clear(self, db_session):
        query = db_session.query(CrudRecordLock).filter(CrudRecordLock.expires < datetime.datetime.utcnow())
        for lock in query:
            db_session.delete(lock)
        db_session.commit()

    def _random_id(self):
        xhash = random.getrandbits(64)
        xstr = "%020d" % xhash
        while len(xstr) > 9 and xstr[0] == '0':
            xstr = xstr[1:]
        xid = int(xstr[:9])
        return xid

    def lock_get_keys(self, db_session, request_params, user_id):
        query = self.base_query(db_session, request_params)
        query = self.add_filter(db_session, request_params, query)
        primary_id = request_params.arguments.get(self._primary_key.name)
        query = self.filter_primary(db_session, request_params, query, primary_id)
        raw_result = query.first()
        if raw_result is None:
            raise CrudResponsePlain("Not found", 404)
        if isinstance(raw_result, (list, tuple)):
            row = raw_result[0]
        else:
            row = raw_result

        if self.is_valid_lock(db_session, request_params, raw_result, user_id) != True:
            raise CrudResponsePlain('User is not allowed to lock record', 403)
        keys = []
        for pk in class_mapper(self._sql_alchemy_model).primary_key:
            keys.append(str(getattr(row, pk.name)))
        return self._sql_alchemy_model.__tablename__, ','.join(keys)

    def lock_aquire(self, db_engine, request_params, resource_dict, reef_user_id):
        user_id = resource_dict.get('user_id', 0)
        if reef_user_id != 0 and int(reef_user_id) != user_id:
            raise CrudResponseJson({'reason': 'Invalid user_id'}, 403)
        with session_scope(db_engine) as session:
            self._lock_clear(session)
            lock_type = resource_dict.get('type', '')
            tab, key = self.lock_get_keys(session, request_params, user_id)
            try:
                lock = CrudRecordLock()
                lock.id = self._random_id()
                lock.table_name = tab
                lock.table_key = key
                lock.lock_type = lock_type
                lock.user_id = resource_dict.get('user_id', 0)
                lock.expires = datetime.datetime.utcnow() + datetime.timedelta(seconds=CrudObject.LOCK_TIME)
                lock.created_at = datetime.datetime.utcnow()
                session.add(lock)
                session.commit()
                return CrudResponsePlain("{}/{}\n".format(request_params.request_uri, lock.id), 201)

            except IntegrityError:
                session.rollback()
                query = session.query(CrudRecordLock).filter(CrudRecordLock.table_name == tab) \
                    .filter(CrudRecordLock.table_key == key) \
                    .filter(CrudRecordLock.lock_type == lock_type)
                for lock in query:
                    resp = {'reason': 'locked', 'user_id': lock.user_id}
                    if lock.user_id == int(resource_dict.get('user_id', 0)):
                        resp['lock_id'] = lock.id
                    return CrudResponseJson(resp, 409)

                return CrudResponseJson({'reason': 'unknown'}, 400)

    def _lock_get_resource(self, db_session, request_params, resource_id, reef_user_id):
        try:
            lock = db_session.query(CrudRecordLock).filter(CrudRecordLock.id == resource_id).one()
            tab, key = self.lock_get_keys(db_session, request_params, reef_user_id)
            if lock.table_name != tab or lock.table_key != key:
                raise CrudResponsePlain("Not Found(2)", 404)
            return lock
        except (MultipleResultsFound, NoResultFound) as e:
            raise CrudResponsePlain(str(e), 404)

    def lock_renew(self, db_engine, request_params, resource_id, reef_user_id):
        with session_scope(db_engine) as session:
            lock = self._lock_get_resource(session, request_params, resource_id, reef_user_id)
            lock.expires = datetime.datetime.utcnow() + datetime.timedelta(seconds=CrudObject.LOCK_TIME)
            session.add(lock)
            session.commit()
            self._lock_clear(session)
            return CrudResponseJson(as_dict(lock), 200)

    def lock_release(self, db_engine, request_params, resource_id, reef_user_id):
        with session_scope(db_engine) as session:
            lock = self._lock_get_resource(session, request_params, resource_id, reef_user_id)
            session.delete(lock)
            session.commit()
            self._lock_clear(session)
            return CrudResponsePlain("Deleted", 200)

    def lock_get(self, db_engine, request_params, reef_user_id):
        with session_scope(db_engine) as session:
            self._lock_clear(session)
            lock_type = request_params.parameters.get('type', '')
            tab, key = self.lock_get_keys(session, request_params, reef_user_id)
            lock = session.query(CrudRecordLock).filter(CrudRecordLock.table_name == tab) \
                .filter(CrudRecordLock.table_key == key) \
                .filter(CrudRecordLock.lock_type == lock_type).first()
            if lock is None:
                raise CrudResponsePlain("Not found", 404)
            return CrudResponseJson(as_dict(lock), 200)

    def lock_get_id(self, db_engine, request_params, resource_id, reef_user_id):
        with session_scope(db_engine) as session:
            self._lock_clear(session)
            lock = self._lock_get_resource(session, request_params, resource_id, reef_user_id)
            return CrudResponseJson(as_dict(lock), 200)


class DualWriteCrud(CrudObject):
    def translate_columns(self):
        pass

    def get_error_message(self, verb, url, resource_id):
        return "DualWriteCrud: Error {} to {} for id {}.".format(verb, url, resource_id)

    def update(self, db_engine, request_params, resource_id, resource_dict):

        payload = self.send_put(db_engine, request_params, resource_id, resource_dict)

        try:
            with session_scope(db_engine) as session:
                self.init(session, request_params)
                if not self.is_valid_put(session, request_params, resource_id, resource_dict):
                    raise CrudResponsePlain("Invalid Update", 400)
                self.update_core(session, request_params, resource_id, resource_dict)
        except (SQLAlchemyError) as e:
            logging.exception(self.get_error_message("PUT", request_params.request_uri, str(resource_id)))
        except (CrudResponseException) as e:
            logging.exception(self.get_error_message("PUT", request_params.request_uri, str(resource_id)))

        return payload

    def send_put(self, db_engine, request_params, resource_id, resource_dict):
        raise NotImplementedError('This method is not implemented yet')

    def delete(self, db_engine, request_params, resource_id):
        try:
            super(DualWriteCrud, self).delete(db_engine, request_params, resource_id)
        except (SQLAlchemyError) as e:
            logging.exception(self.get_error_message("DELETE", request_params.request_uri, str(resource_id)))
        except (CrudResponseException) as e:
            logging.exception(self.get_error_message("DELETE", request_params.request_uri, str(resource_id)))

        try:
            return self.send_delete(resource_id)
        except Exception, e:
            payload = CrudResponsePlain("Unable to delete resource id {}".format(resource_id, 200))

        return payload

    def send_delete(self, resource_id):
        raise NotImplementedError('This method is not implemented yet')