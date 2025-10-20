import sys
from dlx_dl.scripts import alert

def handler(event, context):
    # Different approach here. Feed the arguments through sys.argv
    sys.argv[1:] = ['--pending_time=7200', '--alert_frequency=21600'] 

    try:
        alert.run()
    except Exception as exc:
        print('; '.join(str(exc).split('\n'))) # puts exception text on one line for CloudWatch logs
