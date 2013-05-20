import MySQLdb.cursors
import MySQLdb.connections

class CachingCursorMixIn(MySQLdb.cursors.CursorStoreResultMixIn):
    def _query(self, q):
        print "Query:", q
        print "Stoage dir:", self.storage_dir
        super(CachingCursorMixIn, self)._query(q)


class CachingCursor(CachingCursorMixIn, MySQLdb.cursors.CursorTupleRowsMixIn,
             MySQLdb.cursors.BaseCursor):
    """The default Cursor class, with CursorStoreResultMixIn replaced with
    CachingCursorMixIn"""
    
class CachingConnection(MySQLdb.connections.Connection):
    #MySQLdb.connections.Connection.default_cursor = CachingCursor
    
    def __init__(self, storage_dir, *args, **kwargs):
        CachingCursorMixIn.storage_dir = storage_dir
        kwargs["cursorclass"] = CachingCursor
        super(CachingConnection, self).__init__(*args, **kwargs)
        

def Connect(storage_dir, *args, **kwargs):
    """Factory function for connections.Connection."""
    return CachingConnection(storage_dir, *args, **kwargs)

connect = Connection = Connect
