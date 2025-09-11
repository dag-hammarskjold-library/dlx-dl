import sys, os
from argparse import ArgumentParser
from datetime import datetime, timedelta, timezone
import boto3
from dlx import DB
from dlx.marc import Marc, Bib, Auth
from dlx_dl.util import elapsed, PendingStatus

ap = ArgumentParser()
ap.add_argument('--connect', required=True)
ap.add_argument('--database', required=True)
ap.add_argument('--pending_time', required=True, type=int, help='Send alert if records have been pending more than this number of seconds')
mg = ap.add_mutually_exclusive_group(required=True)
mg.add_argument('--topic_arn', help='AWS SNS topic ARN')
mg.add_argument('--phone_number', help='AWS SNS topic phone number')

def run() -> dict:
    args = ap.parse_args()
    DB.connect(args.connect, database=args.database) if DB.connected is False else None # if testing, already connected to DB
    alert = False
    statuses = []

    # Check bibs and auths for export pending time
    for cls in (Bib, Auth):
        last_updated = cls.from_query({}, sort=[('updated', -1)], limit=1)
        
        # It's been more than two hours since last record updated, indicating system inactivity
        if elapsed(last_updated.updated.replace(tzinfo=timezone.utc)).seconds > 7200:
            continue
        
        # Records have been updated since the last export
        statuses.append(PendingStatus(collection='bibs' if cls == Bib else 'auths'))
    
    message = ''
    
    for status in [x for x in statuses if x.pending_time > args.pending_time]:
        minutes = int(status.pending_time / 60)
        message = message if message else 'Hello, please ignore this test email'
        message += f'\n\n{"Bib" if cls == Bib else "Auth"} exports have been pending for more than {minutes} minutes.'

    if message:
        alert = True
        subject = f'TEST - Warning: exports to UNDL have been pending for more than {minutes} minutes'
        message += '\n\nSee https://cloudwatch.amazonaws.com/dashboard.html?dashboard=DLX-DL&context=eyJSIjoidXMtZWFzdC0xIiwiRCI6ImN3LWRiLTk1MDIzNjUzNzk0OSIsIlUiOiJ1cy1lYXN0LTFfb256a1pUMmphIiwiQyI6IjZycXFqbm1mZzc2c3RyZzc3ZTZiZW1pbmZzIiwiSSI6InVzLWVhc3QtMTpmMjZiMzkxOC1lNDBjLTQwNzktODIyMy0zZWEzMDViZjA2N2IiLCJPIjoiYXJuOmF3czppYW06Ojk1MDIzNjUzNzk0OTpyb2xlL3NlcnZpY2Utcm9sZS9DV0RCU2hhcmluZy1QdWJsaWNSZWFkT25seUFjY2Vzcy1LNEFMMExLUyIsIk0iOiJQdWJsaWMifQ%3D%3D&start=PT12H&end=null#dashboards:'
        print('Sending alert notification that records are out of sync')
        return notify(topic_arn=args.topic_arn, subject=subject, message=message)

    return

def notify(*, topic_arn: str, phone_number=None, subject: str, message: str) -> dict:
    client = boto3.client('sns')
   
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns/client/publish.html
    # can use a fake phone number in mock aws, but not fake arn
    kwargs = {'PhoneNumber': '+15555555555'} if DB.database_name == 'testing' else phone_number or {'TopicArn': topic_arn}
    kwargs.update({'Subject': subject, 'Message': message})
    
    return client.publish(**kwargs)

###

if __name__ == '__main__':
    # The file is being run by the Python interpreter as a script
    run()
elif not sys.argv[1:]:
    # The function is being imported from another script or module
    raise Exception('Arguments must be provided through sys.argv')