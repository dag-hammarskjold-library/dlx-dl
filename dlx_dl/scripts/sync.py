"""Sync DL from DLX"""

import sys, os, re, json, time, argparse, unicodedata, requests, pytz, uuid
from collections import Counter
from copy import deepcopy
from itertools import chain
from warnings import warn
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse, quote, unquote
from boto3 import client as botoclient
from botocore.exceptions import ClientError, NoCredentialsError
from math import inf
from io import StringIO
from xml.etree import ElementTree
from mongomock import MongoClient as MockClient
from pymongo import UpdateOne, DeleteOne
from bson import SON, Regex
from dlx import DB, Config
from dlx.marc import Query, Bib, BibSet, Auth, AuthSet
from dlx.file import File, Identifier
from dlx.util import Tokenizer
from dlx_dl.scripts import export

API_SEARCH_URL = 'https://digitallibrary.un.org/api/v1/search'
API_RECORD_URL = 'https://digitallibrary.un.org/api/v1/record/'
NS = '{http://www.loc.gov/MARC21/slim}'
LOG_COLLECTION = export.LOG_COLLECTION
LANGMAP = {'AR': 'العربية', 'ZH': '中文', 'EN': 'English', 'FR': 'Français', 'RU': 'Русский', 'ES': 'Español', 'T': 'test'}
LANGMAP_REVERSE = {Tokenizer.scrub(v).replace(' ', ''): k for k, v in LANGMAP.items()}

def get_args(**kwargs):
    parser = argparse.ArgumentParser(prog='dlx-dl-sync')
    
    parser.add_argument('--email', help='receive batch results by email instead of callback')
    parser.add_argument('--force', action='store_true')
    parser.add_argument('--modified_since_log', action='store_true')
    parser.add_argument('--limit', help='limit the number of exports', type=int, default=1000)
    parser.add_argument('--time_limit', help='runtime limit in seconds', type=int, default=600)
    parser.add_argument('--queue', action='store_true', help='try to export records in queue and add to queue if export exceeds limits')
    parser.add_argument('--delete_only', action='store_true')
    parser.add_argument('--use_auth_cache', action='store_true')
    parser.add_argument('--missing_only', action='store_true')

    r = parser.add_argument_group('required')
    r.add_argument('--source', required=True, help='an identity to use in the log')
    r.add_argument('--type', required=True, choices=['bib', 'auth'])
    
    q = parser.add_argument_group('criteria', description='one criteria argument is required') 
    qm = q.add_mutually_exclusive_group(required=True)
    qm.add_argument('--modified_from', help='export records modified since date (ISO format)')
    q.add_argument('--modified_to', help='export records modified until date (ISO format) (only valid with --modified_from)')
    qm.add_argument('--modified_within', help='export records modified within the past number of seconds')
    q.add_argument('--modified_until', help='export records modified up until the number of seconds ago (only valid with --modified_within)')
    qm.add_argument('--list', help='file with list of IDs (max 5000)')
    qm.add_argument('--id', help='a single record ID')
    qm.add_argument('--ids', nargs='+', help='variable-length list of record IDs')
    qm.add_argument('--query', help='JSON MongoDB query')
    qm.add_argument('--querystring', help='dlx querystring syntax')

    # get from AWS if not provided
    ssm = botoclient('ssm', region_name='us-east-1')

    def param(name):
        try:
            return ssm.get_parameter(Name=name)['Parameter']['Value']
        except NoCredentialsError:
            warn('in mock environment')
            return 'mocked'
        except ClientError:
            warn('valid AWS credentials not found or unable to connect')
            return None

    c = parser.add_argument_group('credentials', description='these arguments are automatically supplied by AWS SSM if AWS credentials are configured')
    c.add_argument('--connect', default=param('prodISSU-admin-connect-string'), help='MongoDB connection string')
    c.add_argument('--db', default='undlFiles')
    c.add_argument('--api_key', help='UNDL-issued api key', default=param('undl-dhl-metadata-api-key'))
    c.add_argument('--callback_url', help="A URL that can receive the results of a submitted task.", default=param('undl-callback-url'))
    c.add_argument('--nonce_key', help='A validation key that will be passed to and from the UNDL API.', default=param('undl-callback-nonce'))
    
    # if run as function convert args to sys.argv so they can be parsed by ArgumentParser
    if kwargs:
        sys.argv = [sys.argv[0]] # clear any existing command line args

        for key, val in kwargs.items():
            if val == True:
                # boolean args
                sys.argv.append(f'--{key}')
            elif isinstance(val, list):
                sys.argv.append(f'--{key}')
                sys.argv += val
            else:
                sys.argv.append(f'--{key}={val}')
     
    return parser.parse_args()

def run(**kwargs) -> int:
    """
    Main function. Arguments are described and parsed in the `get_args` 
    function. Returns the number of records updated, or -1 if export was
    aborted. 
    """

    args = get_args(**kwargs)

    if isinstance(kwargs.get('connect'), MockClient):
        # required for testing 
        DB.client = kwargs['connect']
    else:
        DB.connect(args.connect, database=args.db)

    args.START = datetime.now(timezone.utc)
    blacklist = DB.handle[export.BLACKLIST_COLLECTION]
    args.blacklisted = [x['symbol'] for x in blacklist.find({})]

    HEADERS = {'Authorization': 'Token ' + args.api_key}
    marcset, deleted = get_records(args) # returns an interator  (dlx.Marc.BibSet/AuthSet)
    TOTAL = marcset.count + len(deleted)
    #deleted = get_deleted_records(args)
    BATCH = []
    BATCH_SIZE = 100
    SEEN = 0
    UPDATED_COUNT = 0
    print(f'Checking {marcset.count} records')

    # check if last update cleared in DL yet
    if args.force:
        pass
    else:
        to_check = 50
        last_n = list(DB.handle[export.LOG_COLLECTION].find({'source': args.source, 'record_type': args.type}, sort=[('time', -1)], limit=to_check)) or []

        if not last_n:
            raise Exception('No log data found for this source. Run with --force to skip this check')

        last_exported = next(filter(lambda x: x.get('response_code') == 200, last_n))

        if not last_exported:
            raise Exception(f'The last {to_check - len(last_n)} exports have been rejected by the DL subission API. Check data and API status')

        # check if any record in the last export were new records
        last_export_start = last_n[0]['export_start']
        
        if last_new := DB.handle[export.LOG_COLLECTION].find_one({'export_start': last_export_start, 'export_type': 'NEW', 'response_code': 200}, sort=[('time', -1)]):
            last_exported = last_new

        # use DL search API to find the record in DL
        pre = '035__a:(DHL)' if args.type == 'bib' else '035__a:(DHLAUTH)'
        url = f'{API_SEARCH_URL}?search_id=&p={pre}{last_exported["record_id"]}&format=xml'

        if args.type == 'auth':
            url += '&c=Authorities'

        if response := requests.get(url, headers=HEADERS):
            root = ElementTree.fromstring(response.text)
            col = root.find(f'{NS}collection')
            record_xml = col.find(f'{NS}record')
        else:
            raise Exception('API request failed')

        # check if the record has been updated in DL yet
        flag = None 
        last = Bib.from_xml(last_exported['xml'], auth_control=False)
                            
        if 'DELETED' in (last.get_value('980', 'a'), last.get_value('980', 'c')):
            try:
                last_dl_record = Bib.from_xml_raw(record_xml, auth_control=False)
                
                # the record is hasn't been purged from DL yet
                if 'DELETED' not in (last_dl_record.get_value('980', 'a'), last_dl_record.get_value('980', 'c')):
                    flag = 'DELETE'
            except AssertionError:
                # the record doesnt exist, presumably already purged
                pass
        else:
            try:
                last_dl_record = Bib.from_xml_raw(record_xml, auth_control=False)
            except AssertionError as e:
                if status := last_exported.get('export_type') == 'NEW':
                    # last record not in DL yet
                    flag = 'NEW'
                    last_dl_record = None
                elif status == None:
                    # record was probably exported by dlx-dl-export
                    pass
                else:
                    raise Exception(f'Last updated record not found by DL search API: {last_exported["record_type"]} {last_exported["record_id"]}')
            
            if last_dl_record:
                # DL record last updated time is in 005
                dl_last_updated = str(int(float(last_dl_record.get_value('005'))))
                dl_last_updated = datetime.strptime(dl_last_updated, '%Y%m%d%H%M%S')
                # 005 is in local time
                dl_last_updated += timedelta(hours=4 if pytz.timezone('US/Eastern').localize(dl_last_updated).dst() else 5)

                if last_exported['time'] > dl_last_updated:
                    flag = 'UPDATE'
        
        if flag:
            # check callback log to see if the last export had an import error in DL
            q = {'record_type': last_exported['record_type'], 'record_id': last_exported['record_id']}

            if export_id := last_exported.get('export_id'):
                q['nonce.export_id'] = export_id
            else:
                # todo: get rid of this when possible to transition to using only export_id
                q['nonce.export_start'] = Regex('^' + str(last_export_start)[:19]) # time strings might not match at microsecond level for some reason

            callback_data = DB.handle[export.CALLBACK_COLLECTION].find_one(q, sort=[('time', -1)])

            if callback_data:
                if callback_data['results'][0]['success'] == False:
                    # the last export was exported succesfully, but failed on import to DL. proceed with export
                    print(f'There was an error in DL processing the last {flag} record. Proceeding.')
                    pass
                elif flag == 'NEW':
                    # the record has been imported to DL but isn't searchable yet
                    print(f'Awaiting search indexing of last new record: {args.type}# {last_exported["record_id"]}. Callback received indicating sucessful import @ {callback_data["time"]}.')
                    return -1
                else:
                    # the record was exported and imported to DL succesfully, but DL did not record the update in
                    # the 005 field. this can happen if there were no changes to be made to the DL record.
                    warn(f'Possible redundant export not recorded in DL: {flag} {args.type}# {last_exported["record_id"]}')
            else:
                print(f'Last update not cleared in DL yet ({flag}) ({args.type}# {last_exported["record_id"]} @ {last_exported["time"]})')
                return -1

    # cycle through records in batches 
    enqueue, to_remove = False, []

    if args.use_auth_cache:
        print('building auth cache...')
        Auth.build_cache()
    
    for i, record in enumerate(chain(marcset.records, (d for d in deleted))):
        if record.user is None:
            record.user = 'system'

        if record.user[:10] == 'batch_edit':
            # skip syncing batch edited records for now so as not to overwhelm DL queue
            continue

        BATCH.append(record)
        SEEN = i + 1
        
        # process DL batch
        if len(BATCH) in (BATCH_SIZE, TOTAL) or SEEN == TOTAL:
            DL_BATCH = []

            # get DL records using DL search API
            pre = '035__a:(DHL)' if args.type == 'bib' else '035__a:(DHLAUTH)'
            terms = ' OR '.join([f'{pre}{r.id}' for r in BATCH])
            url = f'{API_SEARCH_URL}?search_id=&p={terms}&format=xml' #'&ot=035,998'
            
            if args.type == 'auth':
                url += '&c=Authorities'
                
            response = requests.get(url, headers=HEADERS)
            retries = 0
            
            while response.status_code != 200:
                print(f'retrying: {url}\n{response.text}')  
                      
                if retries > 5: 
                    raise Exception(f'search API error: {response.text}')
                
                if 'Max 100 requests per 5 minutes' in json.loads(response.text).get('error'):
                    print('API rate limit exceeded. waiting 5 minutes')
                    time.sleep(310)
                else:
                    time.sleep((retries if retries else 1) * 5)
                
                retries += 1
                response = requests.get(url, headers=HEADERS)
            
            root = ElementTree.fromstring(response.text)
            #search_id = root.find('search_id').text
            col = root.find(f'{NS}collection')
        
            # process DL XML
            for r in [] if col is None else col:
                dl_record = Bib.from_xml_raw(r, auth_control=False, delete_subfield_zero=False)
                _035 = next(filter(lambda x: re.match(r'^\(DHL', x), dl_record.get_values('035', 'a')), '')

                if match := re.match(r'^\((DHL|DHLAUTH)\)(.*)', _035):
                    dl_record.id = int(match.group(2))
                    DL_BATCH.append(dl_record)

            # record not in DL
            for dlx_record in BATCH:
                if dlx_record.get_value('245', 'a')[0:16].lower() == 'work in progress':
                    continue
                
                if dlx_record.get_value('980', 'a') == 'DELETED':
                    if dl_record := next(filter(lambda x: x.id == dlx_record.id, DL_BATCH), None):
                        if dl_record.get_value('980', 'a') != 'DELETED':
                            print(f'{dlx_record.id}: RECORD DELETED')
                            export_whole_record(args, dlx_record, export_type='DELETE')
                            UPDATED_COUNT += 1
                        
                        # remove record from list of DL records to compare
                        DL_BATCH.remove(dl_record)
                elif dlx_record.id not in [x.id for x in DL_BATCH]:
                    print(f'{dlx_record.id}: NOT FOUND IN DL')
                    export_whole_record(args, dlx_record, export_type='NEW')
                    UPDATED_COUNT += 1
                    
                # remove from queue
                to_remove.append(dlx_record.id)

            # end here if only adding missing records
            if args.missing_only:
                # clear batch
                BATCH = []
                continue
            
            # scan and compare DL records
            for dl_record in DL_BATCH:
                dlx_record = next(filter(lambda x: x.id == dl_record.id, BATCH), None)
                
                if dlx_record is None:
                    raise Exception(f'Error matching {dl_record.id} with dlx record. This shouldn\'t be possible. Possible network error.\n{dl_record.to_mrk()}')
                
                # correct fields    
                result = compare_and_update(args, dlx_record=dlx_record, dl_record=dl_record)
                # remove from queue
                to_remove.append(dlx_record.id)
                    
                if result:
                    UPDATED_COUNT += 1
                    
            # clear batch
            BATCH = []
            
            # do the queue removals
            DB.handle[export.QUEUE_COLLECTION].bulk_write([DeleteOne({'type': args.type, 'record_id': x}) for x in to_remove])
            to_remove = []
            
        # status
        print('\b' * (len(str(SEEN)) + 4 + len(str(TOTAL))) + f'{SEEN} / {TOTAL} ', end='', flush=True)

        # limits
        if args.limit != 0 and UPDATED_COUNT >= args.limit:
            print('\nReached max exports')
            enqueue = True if args.queue else False
            break
        if args.time_limit and datetime.now(timezone.utc) > args.START + timedelta(seconds=args.time_limit):
            print('\nTime limit exceeded')
            enqueue = True if args.queue else False
            break

        # end
        if SEEN == TOTAL:
            break

    if enqueue:
        print('Submitting remaining records to the queue... ', end='', flush=True)
        updates = []

        for i, record in enumerate(marcset):
            # records is a map object so the unprocessed records will be left over from the loop break
            data = {'time': datetime.now(timezone.utc), 'source': args.source, 'type': args.type, 'record_id': record.id}
            updates.append(UpdateOne({'source': args.source, 'type': args.type, 'record_id': record.id}, {'$setOnInsert': data}, upsert=True))

        if updates:
            result = DB.handle[export.QUEUE_COLLECTION].bulk_write(updates)
            print(f'{result.upserted_count} added. {i + 1 - result.upserted_count} were already in the queue')

    print(f'Updated {UPDATED_COUNT} records')

    return UPDATED_COUNT

def get_records_by_date(cls, date_from, date_to=None, delete_only=False):
    """
    Returns
    -------
    BibSet / AuthSet
    """
    if cls == BibSet and not delete_only:
        fft_symbols = export._new_file_symbols(date_from, date_to)
    
        if len(fft_symbols) > 100_000:
            raise Exception('that\'s too many file symbols to look up, sorry :(')

        print(f'found files for {len(fft_symbols)} symbols')
    else:
        fft_symbols = None

    fft_uris = export._new_file_uris(date_from, date_to)
    
    if date_to:
        criteria = {'$and': [{'updated': {'$gte': date_from}}, {'updated': {'$lte': date_to}}]}
        history_criteria = {'$and': [{'deleted.time': {'$gte': date_from}}, {'deleted.time': {'$lte': date_to}}, {'deleted.user': {'$ne': 'HZN'}}]}
    else:
        criteria = {'updated': {'$gte': date_from}}
        history_criteria = {'deleted.time': {'$gte': date_from}, 'deleted.user': {'$ne': 'HZN'}}

    if cls == BibSet:
        if fft_symbols:
            query = {
                '$or': [
                    criteria, 
                    {'191.subfields.value': {'$in': fft_symbols}},
                    {'561.subfields.value': {'$in': fft_uris}},
                ]
            }
        else:
            query = criteria
    else:
        query = criteria
    
    # records to delete
    history = DB.handle['bib_history'] if cls == BibSet else DB.handle['auth_history']
    # filter out records that have been restored since they were last deleted. it's easier
    # to do that here than with MQL
    deleted = [
        x for x in history.find(history_criteria)
            if x.get('restored', {}).get('time') or datetime(1970, 1, 1) < x['deleted']['time']
    ]
               
    # sort to ensure latest updates are checked first
    if delete_only:
        # todo: fix this in dlx. MarcSet.count not working unless created by .from_query
        rset = cls.from_query({'_id': {'$exists': False}})
    else:
        rset = cls.from_query(query, sort=[('updated', -1)], collation=Config.marc_index_default_collation)

    to_delete = []

    if deleted:
        rcls = Bib if cls == BibSet else Auth
        
        for d in deleted:
            r = rcls({'_id': d['_id']})
            r.set('980', 'a', 'DELETED')
            r.updated = d['deleted']['time']
            r.user = d['deleted']['user']
            to_delete.append(r)

        rset.records = (r for r in chain((r for r in rset.records), (d for d in  to_delete))) # program is expecting an iterable
        
    print(f'Checking {len(to_delete)} deleted records')

    # todo: enalbe MarcSet.count to handle hybrid cursor/list record sets
  
    return [rset, to_delete]

def get_records(args, log=None, queue=None):
    cls = BibSet if args.type == 'bib' else AuthSet
    since, to = None, None
    deleted = []

    if args.modified_within and args.modified_until:
        since = datetime.utcnow() - timedelta(seconds=int(args.modified_within))
        to = datetime.utcnow() - timedelta(seconds=int(args.modified_until))
        marcset, deleted = get_records_by_date(cls, since, to, delete_only=args.delete_only)
    elif args.modified_within:
        since = datetime.utcnow() - timedelta(seconds=int(args.modified_within))
        marcset, deleted = get_records_by_date(cls, since, None, delete_only=args.delete_only)
    elif args.modified_until:
        raise Exception('--modified_until not valid without --modified_within')
    elif args.modified_from and args.modified_to:
        since = datetime.fromisoformat(args.modified_from)
        to = datetime.fromisoformat(args.modified_to)
        marcset, deleted = get_records_by_date(cls, since, to, delete_only=args.delete_only)
    elif args.modified_from:
        since = datetime.fromisoformat(args.modified_from)
        marcset, deleted = get_records_by_date(cls, since, to, delete_only=args.delete_only)
    elif args.modified_to:
        raise Exception('--modified_to not valid without --modified_from')
    elif args.modified_since_log:
        c = log.find({'source': args.source, 'record_type': args.type, 'export_end': {'$exists': 1}}, sort=[('export_start', -1)], limit=1)
        last = next(c, None)
        if last:
            last_export = last['export_start']
            marcset, deleted = get_records_by_date(cls, last_export, None, delete_only=args.delete_only)
        else:
            warn('Initializing the source log entry and quitting.')
            log.insert_one({'source': args.source, 'record_type': args.type, 'export_start': datetime.now(timezone.utc), 'export_end': datetime.now(timezone.utc)})
            return
    elif args.id:
        marcset = cls.from_query({'_id': int(args.id)})
    elif args.ids:
        marcset = cls.from_query({'_id': {'$in': [int(x) for x in args.ids]}})
    elif args.list:
        with open(args.list, 'r') as f:
            ids = [int(row[0]) for row in [line.split("\t") for line in f.readlines()]]
            if len(ids) > 5000: raise Exception(f'Max 5000 IDs from list')
            marcset = cls.from_query({'_id': {'$in': ids}})
    elif args.query:
        query = args.query.replace('\'', '"')
        marcset = cls.from_query(json.loads(query), collation=Config.marc_index_default_collation)
    elif args.querystring:
        query = Query.from_string(args.querystring, record_type=args.type)
        marcset = cls.from_query(query, collation=Config.marc_index_default_collation)
    else:
        raise Exception('One of the criteria arguments is required')

    if args.queue:
        queue = DB.handle[export.QUEUE_COLLECTION]
        qids = [x['record_id'] for x in queue.find({'source': args.source, 'type': args.type})]
        print(f'Taking {len(qids)} from queue')
        q_args, q_kwargs = marcset.query_params
        marcset = cls.from_query({'$or': [{'_id': {'$in': list(qids)}}, q_args[0]]}, sort=[('updated', 1)])

    return [marcset, deleted]

def normalize(string):
    return unicodedata.normalize('NFD', string)
    
def clean_dlx_values(record):
    for field in record.datafields:
        for sub in filter(lambda x: not hasattr(x, 'xref'), field.subfields):
            if re.match(r'^-+$', sub.value):
                # value can't start with '-'
                sub.value.replace('-', '_')
            elif sub.value == '' or re.match(r'^\s+$', sub.value):
                # value can't be blank
                field.subfields.remove(sub)
                        
        if len(field.subfields) == 0:
            # field must contain subfields
            record.fields.remove(field)

        field.ind1 = ' ' if field.ind1 == '_' else field.ind1
        field.ind2 = ' ' if field.ind2 == '_' else field.ind2
    
    return record

def export_whole_record(args, record, *, export_type):
    if export_type not in ['NEW', 'UPDATE', 'DELETE']:
        raise Exception('invalid "export_type"')

    # perform necessary transformations
    record = clean_dlx_values(record)

    # no comp with DL data performed
    if args.type == 'bib':
        record = export.process_bib(record, blacklisted=args.blacklisted, files_only=False)
    else:
        record = export.process_auth(record)

    return submit_to_dl(args, record, mode='insertorreplace', export_start=args.START, export_type=export_type)

def delete_file(args, record, filename):
    name, extension = os.path.splitext(filename)
    deletion_record = Bib()
    deletion_record.id = record.id
    deletion_record.set('035', 'a', f'(DHL){record.id}' if args.type == 'bib' else f'(DHLAUTH){record.id}')
    deletion_record.set('FFT', 'n', filename)
    deletion_record.set('FFT', 'f', extension)
    deletion_record.set('FFT', 't', 'EXPUNGE')

    submit_to_dl(args, deletion_record, mode='correct', export_start=args.START, export_type='UPDATE')

def compare_and_update(args, *, dlx_record, dl_record):
    dlx_record = clean_dlx_values(dlx_record)
    dlx_record = export._980(dlx_record) # add the 980 to dlx record for comparison
    
    skip_fields = ['035', '909', '949', '998']
    dlx_fields = list(filter(lambda x: x.tag not in skip_fields, dlx_record.datafields))
    dl_fields = list(filter(lambda x: x.tag not in skip_fields, dl_record.datafields))
    take_tags = set()
    delete_fields = []

    # obsolete xrefs
    for field in dl_fields:
        if xref := field.get_value('0'):
            if xref[:9] == '(DHLAUTH)':
                field.set('0', xref[9:]) # dlx records do not have the DHLAUTH prefix
            else:
                print(f'{dlx_record.id}: BAD XREF: {field.to_mrk()}')
                field.subfields = list(filter(lambda x: x.code != '0', field.subfields))
                take_tags.add(field.tag)

    # remove auth controlled subfields with no value (subfield may have been deleted in auth record)
    for field in dlx_fields:
        field.subfields = list(filter(lambda x: x.value is not None, field.subfields))

    # serialize to text for comparison
    dlx_fields_serialized = [x.to_mrk() for x in dlx_fields]
    dl_fields_serialized = [x.to_mrk() for x in dl_fields]

    # dlx -> dl
    for field in dlx_fields:      
        if field.tag == '856':
            url = field.get_value('u')
            
            if urlparse(url).netloc in export.WHITELIST:
                # files in these fields have been sent as FFT
                continue

        if normalize(field.to_mrk()) not in [normalize(x) for x in dl_fields_serialized]:
            print(f'{dlx_record.id}: UPDATE: {field.to_mrk()}')
            take_tags.add(field.tag)

    # dl -> dlx
    for field in dl_fields:
        if field.tag == '856':
            if 'digitallibrary.un.org' in field.get_value('u'):
                # FFT file
                continue

        if normalize(field.to_mrk()) not in [normalize(x) for x in dlx_fields_serialized]:
            # compare tag + indicators
            if field.tag + ''.join(field.indicators) in [x.tag + ''.join(x.indicators) for x in dlx_fields]:
                if field.tag not in take_tags:
                    # this should already be taken care of in dlx->dl
                    print(f'{dlx_record.id}: SUPERSEDED: {field.to_mrk()}')
                    take_tags.add(field.tag)
            else:
                # delete fields where the tag + indicators combo does not exist in dl record
                print(f'{dlx_record.id}: TO DELETE: {field.to_mrk()}')

                # use the field in the export to delete the field in DL by setting values to empty string
                for subfield in field.subfields:
                    if hasattr(subfield, 'xref'):
                        subfield.xref == None
                    
                    subfield.value = ""

                delete_fields.append(field)

    # duplicated dl fields
    dlx_counts = Counter(dlx_fields_serialized)
    dl_counts = Counter(dl_fields_serialized)

    for dup in filter(lambda x: x[1] > 1, dl_counts.items()):
        # check if field is also duplicated in dlx
        # `dup` is a Counter object
        if dlx_counts[dup[0]] != dup[1]:
            print(f'{dlx_record.id}: DUPLICATED FIELD: {dup}')
            tag = dup[0][1:4]
            take_tags.add(tag)

    # for comparing the filenames from dl record 856 with dlx filename
    def _get_dl_856(fn):
        fn = export.clean_fn(fn)

        # chars requiring encoding
        fn = fn.replace('%', '%25')
        #fn = fn.replace('^', '%5E')
        #fn = quote(fn)

        if unquote(fn) == fn:
            fn = quote(fn)

        dl_vals = [x.split('/')[-1] for x in dl_record.get_values('856', 'u')]
        dl_vals = [unquote(x) for x in dl_vals] # filenames in UNDL get double percent encoded?

        # remove extra chars if any
        try:
            # not sure what this is supposed to be doing. to review
            pass # dl_vals = [x[:len(fn)-fn[::-1].index('.')-1] + fn[-fn[::-1].index('.')-1:len(fn)] for x in dl_vals]
        except ValueError:
            pass
        except Exception as e:
            print(f'Error: {dlx_record.id}')
            raise e

        return dl_vals
    
    # collector tool files
    for field in dlx_record.get_fields('856'):
        if field.get_value('3') == 'Thumbnail':
            continue
        
        url = field.get_value('u')

        if urlparse(url).netloc not in export.WHITELIST:
            for s in field.subfields:
                if s.value not in dl_record.get_values('856', s.code):
                    take_tags.add('856')
        else:
            # from Collector Tool
            if len(list(filter(lambda x: 'digitallibrary.un.org' in x, dl_record.get_values('856', 'u')))) == 0:
                print(f'{dlx_record.id}: FILE NOT FOUND ' + url)
                
                return export_whole_record(args, dlx_record, export_type='UPDATE')

            fn = url.split('/')[-1]
            
            if export.clean_fn(fn) not in _get_dl_856(fn):
                print(f'{dlx_record.id}: FILE NOT FOUND ' + url)

                return export_whole_record(args, dlx_record, export_type='UPDATE')

    # for comparing number of files in each system
    all_dlx_files = []

    # records with file URI in 561
    uris = dlx_record.get_values('561', 'u')

    for uri in uris:
        if files := list(File.find_by_identifier(Identifier('uri', uri))):
            latest = sorted(files, key=lambda x: x.timestamp, reverse=True)[0]
            
            if latest.id not in [x.id for x in all_dlx_files]:
                all_dlx_files.append(latest)

            # filename and size should be same in DL
            fn = uri.split('/')[-1]

            if export.clean_fn(fn) not in _get_dl_856(fn):
                print(f'{dlx_record.id}: FILE NOT FOUND ' + uri)

                return export_whole_record(args, dlx_record, export_type='UPDATE')
    
    # official doc files
    symbols = (dlx_record.get_values('191', 'a') + dlx_record.get_values('191', 'z')) if args.type == 'bib' else []
    
    for symbol in set(symbols):
        if symbol == '' or symbol == ' ' or symbol == '***': continue # note: clean these up in db

        for lang in ('AR', 'ZH', 'EN', 'FR', 'RU', 'ES', 'DE'):
            if f := File.latest_by_identifier_language(Identifier('symbol', symbol), lang):
                if f.id not in [x.id for x in all_dlx_files]:
                    all_dlx_files.append(f)

                field = next(filter(lambda x: re.search(fr'{lang}\.\w+$', x.get_value('u')), dl_record.get_fields('856')), None)
                
                if field:
                    try:
                        size = int(field.get_value('s'))
                    except ValueError:
                        size = 0

                    if size != f.size:
                        print(f'{dlx_record.id}: FILE SIZE NOT MATCHING - {symbol}-{lang}')
                        #print([size, f.to_dict()])
                        return export_whole_record(args, dlx_record, export_type='UPDATE')

                if field is None and 'RES' not in dlx_record.get_values('091', 'a') and symbol not in args.blacklisted:
                    print(f'{dlx_record.id}: FILE NOT FOUND - {symbol}-{lang}')
                    
                    return export_whole_record(args, dlx_record, export_type='UPDATE')

    # check if there are a different number of files in DL than DLX
    dl_files = [x for x in dl_record.get_fields('856') if re.match(r'http[s]?://digitallibrary.un.org', x.get_value('u'))]
    # files that came from whitelisted 856 urls are not currently in the dlx filestore 
    dl_file_count = len(dl_files) - len([x for x in dlx_record.get_fields('856') if urlparse(x.get_value('u')).netloc in export.WHITELIST])

    # skip this for now. does not delete extra files in DL
    if False: #dl_file_count != len(all_dlx_files):
        #print(f'EXTRA FILES DETECTED - {[x.to_mrk() for x in dl_files]}\n{[f.to_dict() for f in all_dlx_files]}')
        #return export_whole_record(args, dlx_record, export_type='UPDATE')
        pass
    
    # run api submission
    if take_tags or delete_fields:
        record = Bib() if args.type == 'bib' else Auth()
        record.id = dlx_record.id
        record.set('001', None, dl_record.get_value('001'))
        
        for tag in sorted(list(take_tags)):
            record.fields += dlx_record.get_fields(tag)

        record.fields += delete_fields

        if _998 := dlx_record.get_field('998'):
            record.fields.append(_998)

        return submit_to_dl(args, record, mode='correct', export_start=args.START, export_type='UPDATE')

    return

def submit_to_dl(args, record, *, mode, export_start, export_type):
    if mode not in ('insertorreplace', 'correct'):
        raise Exception('invalid "mode"')

    if export_type not in ('NEW', 'UPDATE', 'DELETE'):
        raise Exception('invalid "export_type"')

    export_id = str(uuid.uuid4()) # random uuid
    xml = record.to_xml(xref_prefix='(DHLAUTH)', write_id=False)
    
    headers = {
        'Authorization': 'Token ' + args.api_key,
        'Content-Type': 'application/xml; charset=utf-8',
    }

    nonce = {'type': args.type, 'id': record.id, 'export_start': str(export_start), 'export_id': export_id,'key': args.nonce_key}
    
    params = {
        'mode': mode,
        'callback_url': args.callback_url,
        'callback_email': 'nofeedback@tind.io',
        'nonce': json.dumps(nonce)
    } 

    response = requests.post(API_RECORD_URL, params=params, headers=headers, data=xml.encode('utf-8'))
    
    logdata = {
        'export_start': export_start,
        'export_id': export_id,
        'export_type': export_type,
        'time': datetime.now(timezone.utc),
        'source': args.source,
        'record_type': args.type, 
        'record_id': record.id, 
        'response_code': response.status_code, 
        'response_text': response.text.replace('\n', ''),
        'xml': xml
    }

    DB.handle[export.LOG_COLLECTION].insert_one(logdata)
    logdata['export_start'] = logdata['export_start'].isoformat()
    logdata['time'] = logdata['time'].isoformat()
    logdata.pop('_id', None)
    print(json.dumps(logdata))

    return logdata

if __name__ == '__main__':
    run()