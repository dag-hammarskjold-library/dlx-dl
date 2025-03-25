import sys, os, traceback, json, pytz
from argparse import ArgumentParser
from datetime import datetime, timezone, timedelta
from time import sleep
from dlx import DB
from dlx.marc import Auth
from dlx_dl.scripts import sync

ap = ArgumentParser('dlx-dl-retro')
ap.add_argument('connect')
ap.add_argument('database')
ap.add_argument('type', choices=['bib', 'auth'])
ap.add_argument('start', type=int)
ap.add_argument('increment', type=int)
ap.add_argument('--force', action='store_true')

def run() -> None:
    args = ap.parse_args()

    DB.connect(args.connect, database=args.database)
    start = int(args.start)
    end = DB.handle[f'{args.type}s'].find_one({}, sort={'_id': -1})['_id']
    increment = int(args.increment) or 1000
    last_updated_count = None

    # worth taking the time to build the cache up front, as this should be a long running process
    Auth.build_cache()

    while 1:
        # loop breaks when max id in the database is reached

        # close and reconnect for each run so the connection isn't open indefitnely
        if DB.connected:
            DB.disconnect()

        # don't run between 2AM and 7PM Monday - Friday
        if not args.force:
            dt = datetime.now(timezone.utc).astimezone(pytz.timezone('America/New_York'))
            tries = 0

            while dt.weekday() < 5 and dt.hour >= 2 and dt.hour < 19:
                if tries == 0:
                    print('sleeping until 7PM...')

                sleep(60 if dt.hour in (18, 19) else 3600)
                tries += 1
                dt = datetime.now(timezone.utc).astimezone(pytz.timezone('America/New_York'))

        # reconnect
        DB.connect(args.connect, database=args.database)

        # don't run if there are records in the dlx-dl queue
        while DB.handle['dlx_dl_queue'].find_one({}):
            print('waiting for queue to clear...')
            sleep(600)

        # run the batch
        query = json.dumps({'$and': [{'_id': {'$gte': start}}, {'_id': {'$lt': start + increment}}]})
        print(f'running {query}')

        try:
            updated_count = sync.run(
                source='dlx-dl-retro', 
                type=args.type, 
                query=query,
                time_limit=0,
                limit=increment
            )
        except Exception as e:
            traceback.print_exc()
            print('Retrying in one minute...')
            sleep(60)
            continue
        
        # disconnect while sleeping
        DB.disconnect()

        # keep track of how meny records are being waited on if run was ab
        if updated_count > 0:
            last_updated_count = updated_count
            
        if updated_count == -1:
            # the run was aborted
            pass
        else:
            # the script completed sucessfully. increment the start id for the next run
            start += increment

        if start > end:
            print(f'done. endend at record id {end}')
            return
        
        # determine how long to wait until the next run
        wait = get_wait_time(updated_count, last_updated_count)

        print(f'waiting {wait / 60} minutes...')
        sleep(wait)

def get_wait_time(updated_count: int, last_updated_count: int) -> int:
    """
    Determine how many seconds to wait until the next run given the number of
    records that were updated. 
    """

    if updated_count == -1:
        # -1 means the last run was aborted due to updates still pending in DL

        if last_updated_count == None:
            # updates from a previous process are still pending
            wait = 300
        elif last_updated_count <= 10:
            wait = 60
        elif last_updated_count <= 50:
            wait = 120
        elif last_updated_count <= 100:
            wait = 180
        else:
            wait = 300
    elif updated_count:
        if updated_count   <= 5:
            wait = 30
        elif updated_count <= 10:
            wait = 60
        elif updated_count <= 25:
            wait = 120
        elif updated_count <= 50:
            wait = 180
        elif updated_count <= 100:
            wait = 300
        else:
            wait = 600
    else:
        wait = 0

    return wait

### 

if __name__ == '__main__':
    run()
