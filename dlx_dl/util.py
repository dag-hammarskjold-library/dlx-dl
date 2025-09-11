
import inspect
from datetime import datetime, timezone, timedelta
from dlx import DB
from dlx.marc import Marc, Bib, BibSet, Auth, AuthSet

# functions
def elapsed(since: datetime, until: datetime = datetime.now(timezone.utc)) -> timedelta:
    """Returns the time elapsed between two datetimes as a timedelta"""

    if since.tzinfo != timezone.utc:
        raise Exception('The timezone for "since" must be UTC')

    return until - since

# classes
class PendingStatus():
    def __init__(self, *, connection_string: str = None, database: str = None, collection: str):
        """Queries the logs and sets the following properties: since, ago, pending_time, pending_records"""

        if connection_string:
            # Not required, because DB may already be connected to
            if not database:
                raise Exception('"database" parameter required with "connection_string')
            
            DB.connect(connection_string, database=database)
        
        if not DB.connected:
            raise Exception('Not connected to DB')

        if collection not in ('bibs', 'auths'): raise Exception('"collection" must be "bibs" or "auths"')
        
        self.collection = collection
        self._pending_time = 0
        self._pending_records = []
        log = DB.handle.get_collection('dlx_dl_log')
        rtype = 'bib' if collection == 'bibs' else 'auth'
        last_exported = log.find_one({'source': 'dlx-dl-lambda', 'record_type': rtype}, sort=[('time', -1)])
        
        if first_updated_since_export := (Bib if collection == 'bibs' else Auth).from_query({'updated': {'$gt': last_exported['time']}}, sort=[('updated', 1)], limit=1):
            # Records have been updated since the last export
            since = first_updated_since_export.updated.replace(tzinfo=timezone.utc)
            self._pending_time = elapsed(since).seconds
            rset = BibSet if collection == 'bibs' else AuthSet
            self._pending_records = list(rset.from_query({'updated': {'$gte': first_updated_since_export.updated}}, sort=[('updated', 1)]))

    @property
    def pending_time(self) -> int:
        """The number of seconds that exports from the collection have been pending"""
        return self._pending_time
    
    @property
    def pending_records(self) -> list[Marc]:
        """The number of seconds that exports from the collection have been pending"""
        return self._pending_records
