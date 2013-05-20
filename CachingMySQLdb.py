import MySQLdb.cursors
import MySQLdb.connections
import os
import hashlib
import cPickle as pickle

class CachingCursorMixIn(MySQLdb.cursors.CursorStoreResultMixIn):

    def _query(self, q):
        """Either queries the database or reads results from a previously
        cached result set"""

        #We only read the results from the cache file if: this is a 
        #select query, and file with correct md5 exists, and file 
        #contains correct query string (this guards against any md5 
        #collisions)

        if q.strip().lower()[:6] != 'select'
            return super(CachingCursorMixIn, self)._query(q)

        md5 = Hashlib.md5(q).hex_digest()
        cache_file = os.path.join(self.storage_dir, md5 + '.txt')

        if os.path.isfile(cache_file):
            handle = open(cache_file, 'r')
            cached_results = pickle.load(handle)
            handle.close()
            if cached_results["query"]  == q:
                print "Read query results from:", cache_file
                self._rows = cached_results["rows"]
                self._result = None
                return len(self._rows)

        #Unable to get a cached result, so run query
        rowcount = self._do_query(q)
        self._rows = self._fetch_row(0)
        self._result = None

        #Before returning, cache results for next time
        cached_results = {"rows" : self._rows, "query" : q}
        handle = open(cache_file, 'r')
        pickle.dump(cached_results, handle)
        handle.close()
        print "Saved query results to:", cache_file        

        return rowcount

class CachingCursor(CachingCursorMixIn, MySQLdb.cursors.CursorTupleRowsMixIn,
             MySQLdb.cursors.BaseCursor):
    """The default Cursor class, with CursorStoreResultMixIn replaced with
    CachingCursorMixIn"""
    
class CachingConnection(MySQLdb.connections.Connection):
    """Drop in for MySQLdb.connections.Connection"""

    def __init__(self, storage_dir, *args, **kwargs):
        storage_dir = os.path.abspath(storage_dir)

        #Create storage directory if it doesn't exist. Notify user.
        if not os.path.isdir(storage_dir):
            os.makedirs(storage_dir)
            print "Creating directory for storage of cached result sets:", storage_dir

        CachingCursorMixIn.storage_dir = storage_dir
        kwargs["cursorclass"] = CachingCursor
        super(CachingConnection, self).__init__(*args, **kwargs)
        

def Connect(storage_dir, *args, **kwargs):
    """Drop in for MySQLdb.Connect, with additional required argument
    for storage directory of sql result sets. Directory will be created
    if it does not exist"""
    return CachingConnection(storage_dir, *args, **kwargs)

connect = Connection = Connect
