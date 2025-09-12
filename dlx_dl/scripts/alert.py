import sys, os, re, warnings
from argparse import ArgumentParser
from datetime import datetime, timedelta, timezone
from boto3 import client
from botocore.exceptions import ClientError, NoCredentialsError
from dlx import DB
from dlx.marc import Marc, Bib, Auth
from dlx_dl.util import elapsed, PendingStatus

SSM = client('ssm')

def param(name):
    return SSM.get_parameter(Name=name)['Parameter']['Value']

AP = ArgumentParser()
AP.add_argument('--connect')
AP.add_argument('--database', default='undlFiles')
AP.add_argument('--pending_time', required=True, type=int, help='Send alert if records have been pending more than this number of seconds')
AP.add_argument('--alert_frequency', type=int, default=21600, help='Skip alert if last alert was within this number of seconds')
mg = AP.add_mutually_exclusive_group(required=True)
mg.add_argument('--topic_arn', help='AWS SNS topic ARN')
mg.add_argument('--phone_number', help='AWS SNS topic phone number')

def run() -> dict:
    args = AP.parse_args()
    args.connect = args.connect or param('prodISSU-admin-connect-string')
    DB.connect(args.connect, database=args.database) if DB.connected is False else None # if testing, already connected to DB
    statuses = []

    # Check bibs and auths for export pending time
    for cls in (Bib, Auth):
        last_updated = cls.from_query({}, sort=[('updated', -1)], limit=1)
        
        # It's been more than two hours since last record updated, indicating system inactivity
        if elapsed(last_updated.updated.replace(tzinfo=timezone.utc)).seconds > 7200:
            continue
        
        status = PendingStatus(collection='bibs' if cls == Bib else 'auths')
        print({status.collection: status.pending_time})
        statuses.append(status)
    
    if statuses := [x for x in statuses if x.pending_time > args.pending_time]:
        # At least one collection has exports pending longer than the max alert time
        alert_collection = DB.handle.get_collection('dlx_dl_alert')
        
        # Skip if alerts have been sent within the alert frequency
        skip = None

        if len(statuses) == 2:
            last_bib_alert = alert_collection.find_one({'collection': 'bibs'}, sort=[('time', -1)]) or {'time': datetime.min}
            last_auth_alert = alert_collection.find_one({'collection': 'auths'}, sort=[('time', -1)]) or {'time': datetime.min}

            if all([elapsed((x.get('time')).replace(tzinfo=timezone.utc)).total_seconds() < args.alert_frequency for x in (last_bib_alert, last_auth_alert)]):
                skip = True
        elif last_alert := alert_collection.find_one({'collection': statuses[0].collection}, sort=[('time', -1)]):
            if elapsed(last_alert.get('time').replace(tzinfo=timezone.utc)).total_seconds() < args.alert_frequency:
                skip = True

        if skip:
            print(f'Exports are pending, but skipping notification due to the last notifcation being within the set alert frequency ({args.alert_frequency})')
            print([{x.collection: x.pending_time} for x in statuses])
            return 
        
        return notify(topic_arn=args.topic_arn, statuses=statuses)

    print(f'No exports pending for longer than the set time ({args.pending_time})')
    return

def notify(*, topic_arn: str, phone_number=None, statuses: list[PendingStatus] = []) -> dict:
    if not statuses:
        return 
    
    sns = client('sns')
    message = ''
    max_minutes = 0

    for status in statuses:
        minutes = int(status.pending_time / 60)
        max_minutes = max_minutes if max_minutes > minutes else minutes
        message = message or 'Hello,'
        message += f'\n\n{"Bib" if status.collection == "bibs" else "Auth"} exports have been pending for more than {minutes} minutes.'
    
    print('Sending message: "{message}"')
    subject = f'UNDL exports pending: exports to UNDL have been pending for more than {max_minutes} minutes'
    message += '\n\nSee https://cloudwatch.amazonaws.com/dashboard.html?dashboard=DLX-DL&context=eyJSIjoidXMtZWFzdC0xIiwiRCI6ImN3LWRiLTk1MDIzNjUzNzk0OSIsIlUiOiJ1cy1lYXN0LTFfb256a1pUMmphIiwiQyI6IjZycXFqbm1mZzc2c3RyZzc3ZTZiZW1pbmZzIiwiSSI6InVzLWVhc3QtMTpmMjZiMzkxOC1lNDBjLTQwNzktODIyMy0zZWEzMDViZjA2N2IiLCJPIjoiYXJuOmF3czppYW06Ojk1MDIzNjUzNzk0OTpyb2xlL3NlcnZpY2Utcm9sZS9DV0RCU2hhcmluZy1QdWJsaWNSZWFkT25seUFjY2Vzcy1LNEFMMExLUyIsIk0iOiJQdWJsaWMifQ%3D%3D&start=PT12H&end=null#dashboards:'
    
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns/client/publish.html
    # Can use a fake phone number in mock aws, but not fake arn
    kwargs = {'PhoneNumber': '+15555555555'} if DB.database_name == 'testing' else phone_number or {'TopicArn': topic_arn}
    kwargs.update({'Subject': subject, 'Message': message})

    if result := sns.publish(**kwargs):
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