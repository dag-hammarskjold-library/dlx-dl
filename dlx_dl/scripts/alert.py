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

    for cls in (Bib, Auth):
        last_updated_record = cls.from_query({}, sort=[('updated', -1)], limit=1)

        if elapsed(last_updated_record.updated).seconds < 7200:
            
            # Records have been updated in the last two hours
            last_exported = log.find_one({'source': 'dlx-dl-lambda', 'record_type': 'bib' if cls == Bib else 'auth'}, sort=[('time', -1)])

            if elapsed(last_exported['time']).seconds > 7200:
                # It's been more than two hours since records were exported to UNDL
                alert = True
                message = f"It's been more than two hours between {'bib' if cls == Bib else 'auth'} updates in Central DB and exports to UNDL"
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