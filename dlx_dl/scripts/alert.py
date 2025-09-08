import sys, os
from argparse import ArgumentParser
from datetime import datetime, timezone, timedelta
import boto3
from dlx import DB
from dlx.marc import Bib, Auth

ap = ArgumentParser()
ap.add_argument('--connect')
ap.add_argument('--database')

def run() -> bool:
    args = ap.parse_args()
    DB.connect(args.connect, database=args.database) if DB.connected is False else None # if testing, already connected to DB
    log = DB.handle.get_collection('dlx_dl_log')
    alert = False

    # Check bibs and auths for export pending time
    for cls in (Bib, Auth):
        last_exported = log.find_one({'source': 'dlx-dl-lambda', 'record_type': 'bib' if cls == Bib else 'auth'}, sort=[('time', -1)])
        last_updated = cls.from_query({}, sort=[('updated', -1)], limit=1)
        
        # It's been more than two hours since last record updated, indicating system inactivity
        if elapsed(last_updated.updated).seconds > 7200:
            continue

        # Records have been updated since the last export
        if updated_since_export := cls.from_query({'updated': {'$gt': last_exported['time']}}, sort=[('updated', 1)], limit=1):
            pending_seconds = elapsed(updated_since_export.updated).seconds

            # Updates have been pending for more than two hours
            if pending_seconds > 7200:
                alert = True
                message = f"It's been more than {int(pending_seconds / 60)} hours between {'bib' if cls == Bib else 'auth'} updates in Central DB and exports to UNDL"
                print('Sending alert notification that records are out of sync')
                notify(message)

    return alert
                
def elapsed(since: datetime, until: datetime = datetime.now(timezone.utc)) -> timedelta:
    """Returns the time elapsed between two datetimes as a timedelta"""

    # Timezones have to be set to subtract datetimes. Assume both utc
    until, since = [x.replace(tzinfo=timezone.utc) for x in (until, since)]

    return until - since

def notify(message: str) -> bool:
    client = boto3.client('sns')
   
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns/client/publish.html
    response = client.publish(
        Message=message,
        # need a Topic, Target or Phone Number set up in AWS SNS
        #TopicArn='x:x:x:x:x' if DB.database_name == 'testing' else '?', # can't use fake TopicArn in tests?
        #TargetArn='string',
        PhoneNumber='5555555555' if DB.database_name == 'testing' else None, 
        
        # below not required
        #Subject='string',
        #MessageStructure='string',
        #MessageAttributes={
        #    'string': {
        #        'DataType': 'string',
        #        'StringValue': 'string',
        #        'BinaryValue': b'bytes'
        #    }
        #},
        #MessageDeduplicationId='string',
        #MessageGroupId='string'
    )

if __name__ == '__main__':
    run()