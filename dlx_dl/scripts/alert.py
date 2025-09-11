import sys, os, re
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
ap.add_argument('--alert_frequency', type=int, default=21600, help='Skip alert if last alert was within this number of seconds')
mg = ap.add_mutually_exclusive_group(required=True)
mg.add_argument('--topic_arn', help='AWS SNS topic ARN')
mg.add_argument('--phone_number', help='AWS SNS topic phone number')

def run() -> dict:
    args = ap.parse_args()
    DB.connect(args.connect, database=args.database) if DB.connected is False else None # if testing, already connected to DB
    statuses = []

    # Check bibs and auths for export pending time
    for cls in (Bib, Auth):
        last_updated = cls.from_query({}, sort=[('updated', -1)], limit=1)
        
        # It's been more than two hours since last record updated, indicating system inactivity
        if elapsed(last_updated.updated.replace(tzinfo=timezone.utc)).seconds > 7200:
            continue
        
        statuses.append(PendingStatus(collection='bibs' if cls == Bib else 'auths'))
    
    if statuses := [x for x in statuses if x.pending_time > args.pending_time]:
        # At least one collection has exports pending longer than the max alert time
        alert_collection = DB.handle.get_collection('dlx_dl_alert')
        
        # Abort if alerts have been sent within the alert frequency
        if len(statuses) == 2:
            last_bib_alert = alert_collection.find_one({'collection': 'bibs'}, sort=[('time', -1)]) or {'time': datetime.min}
            last_auth_alert = alert_collection.find_one({'collection': 'auths'}, sort=[('time', -1)]) or {'time': datetime.min}

            if all([elapsed((x.get('time')).replace(tzinfo=timezone.utc)).total_seconds() < args.alert_frequency for x in (last_bib_alert, last_auth_alert)]):
                return
        elif last_alert := alert_collection.find_one({'collection': statuses[0].collection}):
            if elapsed(last_alert.get('time').replace(tzinfo=timezone.utc)).total_seconds() < args.alert_frequency:
                return

        return notify(topic_arn=args.topic_arn, statuses=statuses)

    return

def notify(*, topic_arn: str, phone_number=None, statuses: list[PendingStatus] = []) -> dict:
    if not statuses:
        return 
    
    client = boto3.client('sns')
    message = ''

    for status in statuses:
        minutes = int(status.pending_time / 60)
        message = message if message else 'Hello,'
        message += f'\n\n{"Bib" if status.collection == 'bibs' else "Auth"} exports have been pending for more than {minutes} minutes.'
    
    if message:
        subject = f'UNDL exports pending: exports to UNDL have been pending for more than {minutes} minutes'
        message += '\n\nSee https://cloudwatch.amazonaws.com/dashboard.html?dashboard=DLX-DL&context=eyJSIjoidXMtZWFzdC0xIiwiRCI6ImN3LWRiLTk1MDIzNjUzNzk0OSIsIlUiOiJ1cy1lYXN0LTFfb256a1pUMmphIiwiQyI6IjZycXFqbm1mZzc2c3RyZzc3ZTZiZW1pbmZzIiwiSSI6InVzLWVhc3QtMTpmMjZiMzkxOC1lNDBjLTQwNzktODIyMy0zZWEzMDViZjA2N2IiLCJPIjoiYXJuOmF3czppYW06Ojk1MDIzNjUzNzk0OTpyb2xlL3NlcnZpY2Utcm9sZS9DV0RCU2hhcmluZy1QdWJsaWNSZWFkT25seUFjY2Vzcy1LNEFMMExLUyIsIk0iOiJQdWJsaWMifQ%3D%3D&start=PT12H&end=null#dashboards:'
    
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns/client/publish.html
    # Can use a fake phone number in mock aws, but not fake arn
    kwargs = {'PhoneNumber': '+15555555555'} if DB.database_name == 'testing' else phone_number or {'TopicArn': topic_arn}
    kwargs.update({'Subject': subject, 'Message': message})

    if result := client.publish(**kwargs):
        for status in statuses:
            print('update db')

            DB.handle.get_collection('dlx_dl_alert').insert_one(
                {
                    'time': datetime.now(timezone.utc),
                    'collection': status.collection,
                    'pending_time': status.pending_time
                }
            )

        return result

###

if __name__ == '__main__':
    # The file is being run by the Python interpreter as a script
    run()
elif not sys.argv[1:]:
    # The function is being imported from another script or module
    raise Exception('Arguments must be provided through sys.argv')