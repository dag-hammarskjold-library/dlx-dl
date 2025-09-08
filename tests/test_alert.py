import sys, pytest
import boto3
from moto import mock_aws
from datetime import datetime, timezone, timedelta
from dlx.marc import DB, Bib, Auth

@pytest.fixture
def db():
    DB.connect('mongomock://localhost') # mock DB
    data = {'_id': 1}
    data['updated'] = datetime.now(timezone.utc) - timedelta(hours=1)
    DB.bibs.insert_one(data)
    data = {'_id': 1}
    data['updated'] = datetime.now(timezone.utc) - timedelta(hours=1)
    DB.auths.insert_one(data)

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

    # four hours since last bib and auth export
    sys.argv[1:] = ['--connect', 'mongomock://localhost']
    assert alert.run() is True

    # a bib has been exported within two hours, but not auth
    DB.handle['dlx_dl_log'].insert_one(
        {
            'record_type': 'bib',
            'time': datetime.now(timezone.utc) - timedelta(hours=1),
            'source': 'dlx-dl-lambda'
        },
    )
    assert alert.run() is True

    # auths and bibs have been exported within two hours. no alert
    DB.handle['dlx_dl_log'].insert_one(
        {
            'record_type': 'auth',
            'time': datetime.now(timezone.utc) - timedelta(hours=1),
            'source': 'dlx-dl-lambda'
        },
    )
    assert alert.run() is False



