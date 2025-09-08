import sys, pytest
import boto3
from moto import mock_aws
from datetime import datetime, timezone, timedelta
from dlx.marc import DB, Bib, Auth

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

@mock_aws
def test_run(db):
    from dlx_dl.scripts import alert

    # Records have been pending for more than two hours 
    sys.argv[1:] = ['--connect', 'mongomock://localhost']
    assert alert.run() is True

    # A bib has been exported within two hours, but not auth
    DB.handle['dlx_dl_log'].insert_one(
        {
            'record_type': 'bib',
            'time': datetime.now(timezone.utc) - timedelta(hours=1),
            'source': 'dlx-dl-lambda'
        },
    )
    assert alert.run() is True

    # Auths and bibs have been pending less than two hours. No alert
    DB.handle['dlx_dl_log'].insert_one(
        {
            'record_type': 'auth',
            'time': datetime.now(timezone.utc) - timedelta(hours=1),
            'source': 'dlx-dl-lambda'
        },
    )
    assert alert.run() is False



