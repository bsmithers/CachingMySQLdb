import MySQLdb.cursors
import MySQLdb.connections
import os
import hashlib
import cPickle as pickle
import time

class CachingCursorMixIn(MySQLdb.cursors.CursorStoreResultMixIn):

    def execute(self, query, args=None, cache=True, acceptable_age=None):
        """Execute the given query using the parent class. Override
        operation of caching or set a specific timeout for this query only"""
        self.do_cache = cache
        default_acceptable_age = self.acceptable_age
        if acceptable_age is not None:
            self.acceptable_age = acceptable_age

        retval = super(CachingCursorMixIn, self).execute(query, args)

        #Reset parameters
        self.do_cache = True
        self.acceptable_age = default_acceptable_age

        return retval

    def _query(self, q):
        """Either queries the database or reads results from a previously
        cache result set"""

        if not self.do_cache or q.strip().lower()[:6] != 'select':
            return super(CachingCursorMixIn, self)._query(q)

        self._last_executed = q

        md5 = hashlib.md5(q).hexdigest()
        cache_file = os.path.join(self.storage_dir, md5 + '.txt')

        if os.path.isfile(cache_file):
            handle = open(cache_file, 'r')
            cached_results = pickle.load(handle)
            handle.close()
            if cached_results["query"]  == q and (self.acceptable_age == 0 or time.time() - cached_results["timestamp"] <= self.acceptable_age):
                # Set all relevant variables - those set inside _do_get_result, except result
                # which we handle differently as it can't be pickled.
                self._rows = cached_results["rows"]
                self._result = None
                self.rowcount = cached_results["rowcount"]
                self.rownumber = cached_results["rownumber"]
                self.description = cached_results["description"]
                self.description_flags = cached_results["description_flags"]
                self.lastrowid = cached_results["lastrowid"]
                self._warnings = cached_results["warnings"]
                self._info = cached_results["info"]
                return self.rowcount

        #Unable to get a cached result, so run query via parent
        retval = super(CachingCursorMixIn, self)._query(q)

        #Before returning, cache results for next time
        cached_results = {}
        cached_results["query"] = q
        cached_results["rows"] = self._rows
        cached_results["rowcount"] = self.rowcount
        cached_results["rownumber"] = self.rownumber
        cached_results["description"] = self.description
        cached_results["description_flags"] = self.description_flags
        cached_results["lastrowid"] = self.lastrowid
        cached_results["warnings"] = self._warnings
        cached_results["info"] = self._info
        cached_results["timestamp"] = time.time()

        handle = open(cache_file, 'w')
        pickle.dump(cached_results, handle)
        handle.close()

        return retval

class CachingCursor(CachingCursorMixIn, MySQLdb.cursors.CursorTupleRowsMixIn,
             MySQLdb.cursors.BaseCursor):
    """The default Cursor class, with CursorStoreResultMixIn replaced with
    CachingCursorMixIn"""
    
class CachingConnection(MySQLdb.connections.Connection):
    """Drop in for MySQLdb.connections.Connection"""

    def __init__(self, storage_dir, acceptable_age, *args, **kwargs):
        storage_dir = os.path.abspath(storage_dir)

        #Create storage directory if it doesn't exist. Notify user.
        if not os.path.isdir(storage_dir):
            os.makedirs(storage_dir)
            print "Creating directory for storage of cached result sets:", storage_dir

        CachingCursorMixIn.storage_dir = storage_dir
        CachingCursorMixIn.acceptable_age = acceptable_age
        CachingCursorMixIn.do_cache = True

        kwargs["cursorclass"] = CachingCursor
        super(CachingConnection, self).__init__(*args, **kwargs)
        

def Connect(storage_dir, acceptable_age=0, *args, **kwargs):
    """Drop in for MySQLdb.Connect, with additional arguments:
    storage_dir: storage directory of sql result sets. Directory will be created
    if it does not exist
    acceptable_age: number of seconds old the cached verion is allowed to be.
    Default = 0 = any age"""
    return CachingConnection(storage_dir, acceptable_age, *args, **kwargs)

connect = Connection = Connect
