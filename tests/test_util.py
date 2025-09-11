import pytest, math
from datetime import datetime, timezone, timedelta
from dlx import DB

@pytest.fixture
def db():
    DB.connect('mongomock://localhost') # mock DB
    
    for col in (DB.bibs, DB.auths):
        # Two records in both cols: first updated 3 hours ago, second updated 1 hour ago
        col.insert_many([
            {'_id': x, 'updated': datetime.now(timezone.utc) - timedelta(hours=y)} for x, y in [(1, 3), (2, 1)]
        ])

    # Last export: 4 hours ago
    DB.handle['dlx_dl_log'].insert_many(
        [
            {
                'record_type': 'bib',
                'time': datetime.now(timezone.utc) - timedelta(hours=4),
                'source': 'dlx-dl-lambda'
            },
            {
                'record_type': 'auth',
                'time': datetime.now(timezone.utc) - timedelta(hours=4),
                'source': 'dlx-dl-lambda'
            },
        ]
    )

    return DB.client

def test_pending_status(db):
    from dlx_dl.util import PendingStatus

    status = PendingStatus(collection='bibs')
    assert round(status.pending_time / 60 / 60) == 3 # round to 3 hours
    assert len(status.pending_records) == 2

    DB.handle['dlx_dl_log'].insert_one(
        {
            'record_type': 'bib',
            'time': datetime.now(timezone.utc) - timedelta(hours=1),
            'source': 'dlx-dl-lambda'
        }
    )

    status = PendingStatus(collection='bibs')
    assert status.pending_time == 0
    assert len(status.pending_records) == 0