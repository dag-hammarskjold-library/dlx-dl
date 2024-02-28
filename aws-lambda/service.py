import json
from dlx_dl.scripts import sync

def handler(event, context): 
    # event dict should contain the params expected by dlx.scripts.sync
    
    print(f'running with args: {json.dumps(event)}')
    
    try:
        sync.run(**event)
    except Exception as exc:
        print('; '.join(str(exc).split('\n'))) # puts exception text on one line for CloudWatch logs

