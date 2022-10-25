"""Sync DL from DLX"""

from operator import index
import sys, os, re, json, time, argparse, unicodedata, requests
from warnings import warn
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse, quote, unquote
from boto3 import client as botoclient
from botocore.exceptions import ClientError
from math import inf
from requests import get
from io import StringIO
from xml.etree import ElementTree
from pymongo import ASCENDING as ASC, DESCENDING as DESC
from bson import SON
from dlx import DB, Config
from dlx.marc import Query, Bib, BibSet, Auth, AuthSet, Datafield
from dlx.file import File, Identifier
import dlx_dl

API_SEARCH_URL = 'https://digitallibrary.un.org/api/v1/search'
API_RECORD_URL = 'https://digitallibrary.un.org/api/v1/record/'
NS = '{http://www.loc.gov/MARC21/slim}'
LOG_COLLECTION = dlx_dl.LOG_COLLECTION

def get_args(**kwargs):
    parser = argparse.ArgumentParser(prog='dlx-dl-sync')
    
    parser.add_argument('--limit', help='limit the number of exports', type=int, default=0)
    parser.add_argument('--email', help='receive batch results by email instead of callback')
    parser.add_argument('--delete_only', action='store_true')
    parser.add_argument('--force', action='store_true')
    
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
        except ClientError:
            warn('valid AWS credentials not found or unable to connect')
            return None

    c = parser.add_argument_group('credentials', description='these arguments are automatically supplied by AWS SSM if AWS credentials are configured')
    c.add_argument('--connect', default=param('connect-string'), help='MongoDB connection string')
    c.add_argument('--api_key', help='UNDL-issued api key', default=param('undl-dhl-metadata-api-key'))
    c.add_argument('--callback_url', help="A URL that can receive the results of a submitted task.", default=param('undl-callback-url'))
    c.add_argument('--nonce_key', help='A validation key that will be passed to and from the UNDL API.', default=param('undl-callback-nonce'))
    
    # if run as function convert args to sys.argv
    if kwargs:
        params = ('ids', 'delete_only', 'force')
        ids, force = [kwargs.get(x) and kwargs.pop(x) for x in params]
        
        sys.argv[1:] = ['--{}={}'.format(key, val) for key, val in kwargs.items()]
        
        # boolean args
        if force: sys.argv.append('--modified_since_log')

        # list args
        if ids:
            sys.argv.append('--ids')
            sys.argv += ids
     
    return parser.parse_args()
    
def run():
    args = get_args()
    args.START = datetime.now(timezone.utc)
    
    DB.connect(args.connect)
    blacklist = DB.handle[dlx_dl.BLACKLIST_COLLECTION]
    args.blacklisted = [x['symbol'] for x in blacklist.find({})]
    args.log = DB.handle[LOG_COLLECTION]


    HEADERS = {'Authorization': 'Token ' + args.api_key}
    
    records = get_records(args)
    BATCH, BATCH_SIZE, SEEN, TOTAL, INDEX = [], 100, 0, records.count, {}
    updated_count = 0
    print(f'checking {TOTAL} records')

    # check if last update indexed in DL yet
    last = args.log.find_one({'source': args.source, 'record_type': args.type}, sort=[('time', DESC)]) or {}
    
    if last_new := args.log.find_one({'export_start': last.get('export_start') or 'X', 'export_type': 'NEW'}, sort=[('time', DESC)]):
        last = last_new

    if args.force:
        pass
    elif last is None:
        raise Exception('No log data found for this source')
    elif (datetime.now() - last['time']) > timedelta(hours=2): # skip check if more than 2 hours
        print("wait time limit exceeded for last import confirmation. proceeding")
    elif last:
        pre = '035__a:(DHL)' if args.type == 'bib' else '035__a:(DHLAUTH)'
        url = f'{API_SEARCH_URL}?search_id=&p={pre}{last["record_id"]}&format=xml'

        if args.type == 'auth':
            url += '&c=Authorities'

        if response := get(url, headers=HEADERS):
            root = ElementTree.fromstring(response.text)
            col = root.find(f'{NS}collection')
            record = col.find(f'{NS}record')
        else:
            raise Exception('API request failed')

        if Bib.from_xml(last['xml']).get_value('980', 'a') == 'DELETED':
            try:
                record = Bib.from_xml_raw(record)
                
                if record.get_value('980', 'a') != 'DELETED':
                    print(f'last update not cleared in DL yet (DELETE) ({args.type}# {last["record_id"]} @ {last["time"]})')
                    exit()

            except AssertionError:
                pass
        else:
            try:
                record = Bib.from_xml_raw(record)
            except AssertionError as e:
                # last record not in DL yet
                print(f'last update not cleared in DL yet (NEW) ({args.type}# {last["record_id"]} @ {last["time"]})')
                exit()

            dl_last = str(int(float(record.get_value('005'))))
            dl_last = datetime.strptime(dl_last, '%Y%m%d%H%M%S')
            # 005 is in local time
            dl_last += timedelta(hours=4 if dl_last.dst else 5)

            if not dl_last > last['time']:
                print(f'last update not cleared in DL yet ({args.type}# {last["record_id"]} @ {last["time"]})')
                exit()

    # cycle through records in batches    
    for record in records:
        BATCH.append(record)
        SEEN += 1
        
        # process batch
        if len(BATCH) in (BATCH_SIZE, TOTAL) or SEEN == TOTAL:
            DL_BATCH = []
            pre = '035__a:(DHL)' if args.type == 'bib' else '035__a:(DHLAUTH)'
            terms = ' OR '.join([f'{pre}{r.id}' for r in BATCH])
            url = f'{API_SEARCH_URL}?search_id=&p={terms}&format=xml' #'&ot=035,998'
            
            if args.type == 'auth':
                url += '&c=Authorities'
                
            response = get(url, headers=HEADERS)
            retries = 0
            
            while response.status_code != 200:
                print('retrying')        
                if retries > 5: 
                    raise Exception(f'search API error: {response.text}')
                    
                time.sleep(5 * retries)
                retries += 1
                response = get(url, headers=HEADERS)
            
            #records = (BibSet if args.type == 'bib' else AuthSet).from_xml(response.text)
            root = ElementTree.fromstring(response.text)
            #search_id = root.find('search_id').text
            col = root.find(f'{NS}collection')
        
            # process DL XML
            for r in col or []:
                dl_record = Bib.from_xml_raw(r)
                DL_BATCH.append(dl_record)
                
                _035 = next(filter(lambda x: re.match('^\(DHL', x), dl_record.get_values('035', 'a')), None)

                if match := re.match('^\((DHL|DHLAUTH)\)(.*)', _035):
                    dl_record.id = int(match.group(2))
 
                # check xrefs
                for f in filter(lambda x: isinstance(x, Datafield), dl_record.fields):
                    if xref := f.get_value('0'):
                        if xref[:9] != '(DHLAUTH)':
                            for s in f.subfields:
                                s.value = 'BAD XREF'

            # record not in DL
            for dlx_record in BATCH:
                if dlx_record.get_value('245', 'a')[0:16].lower() == 'work in progress':
                    continue

                if dlx_record.id not in [x.id for x in DL_BATCH]:
                    print(f'{dlx_record.id}: NOT FOUND IN DL')
                    
                    #exit()
                    export_whole_record(args, dlx_record, export_type='NEW')
                    updated_count += 1
            
            # scan DL records
            for dl_record in DL_BATCH:
                dlx_record = next(filter(lambda x: x.id == dl_record.id, BATCH), None)
                
                if dlx_record is None:
                    raise Exception('This shouldn\'t be possible. Possible network error.')
                    
                if INDEX.get(dlx_record.id):
                    continue
                else:
                    INDEX[dlx_record.id] = True

                # correct fields    
                result = compare_and_update(args, dlx_record=dlx_record, dl_record=dl_record)
                    
                if result:
                    updated_count += 1

            ###
            
            BATCH = []
            
        # status
        print('\b' * (len(str(SEEN)) + 4 + len(str(TOTAL))) + f'{SEEN} / {TOTAL} ', end='', flush=True)
        
        # end
        if SEEN == TOTAL:
            print(f'updated {updated_count} records')
            exit()

        if updated_count > (args.limit if args.limit > 0 else 1000):
            print('Reached max exports')
            break

    print(f'updated {updated_count} records')
        
def get_records(args, log=None, queue=None):
    cls = BibSet if args.type == 'bib' else AuthSet
    since, to = None, None

    if args.modified_within and args.modified_until:
        since = datetime.utcnow() - timedelta(seconds=int(args.modified_within))
        to = datetime.utcnow() - timedelta(seconds=int(args.modified_until))
        records = get_records_by_date(cls, since, to, delete_only=args.delete_only)
    elif args.modified_within:
        since = datetime.utcnow() - timedelta(seconds=int(args.modified_within))
        records = get_records_by_date(cls, since, None, delete_only=args.delete_only)
    elif args.modified_until:
        raise Exception('--modified_until not valid without --modified_within')
    elif args.modified_from and args.modified_to:
        since = datetime.fromisoformat(args.modified_from)
        to = datetime.fromisoformat(args.modified_to)
        records = get_records_by_date(cls, since, to, delete_only=args.delete_only)
    elif args.modified_from:
        since = datetime.fromisoformat(args.modified_from)
        records = get_records_by_date(cls, since, to, delete_only=args.delete_only)
    elif args.modified_to:
        raise Exception('--modified_to not valid without --modified_within')
    elif args.modified_since_log:
        c = log.find({'source': args.source, 'record_type': args.type, 'export_end': {'$exists': 1}}, sort=[('export_start', DESCENDING)], limit=1)
        last = next(c, None)
        if last:
            last_export = last['export_start']
            records = get_records_by_date(cls, last_export, None, delete_only=args.delete_only)
        else:
            warn('Initializing the source log entry and quitting.')
            log.insert_one({'source': args.source, 'record_type': args.type, 'export_start': datetime.now(timezone.utc), 'export_end': datetime.now(timezone.utc)})
            return
    elif args.id:
        records = cls.from_query({'_id': int(args.id)})
    elif args.ids:
        records = cls.from_query({'_id': {'$in': [int(x) for x in args.ids]}})
    elif args.list:
        with open(args.list, 'r') as f:
            ids = [int(row[0]) for row in [line.split("\t") for line in f.readlines()]]
            if len(ids) > 5000: raise Exception(f'Max 5000 IDs from list')
            records = cls.from_query({'_id': {'$in': ids}})
    elif args.query:
        query = args.query.replace('\'', '"')
        records = cls.from_query(json.loads(query))
    elif args.querystring:
        query = Query.from_string(args.querystring, record_type=args.type)
        records = cls.from_query(query)
    else:
        raise Exception('One of the criteria arguments is required')

    hist = DB.handle['bib_history'] if cls == BibSet else DB.handle['auth_history']
        
    return records
    
def get_records_by_date(cls, date_from, date_to=None, delete_only=False):
    """
    Returns
    -------
    BibSet / AuthSet
    """
    if cls == BibSet:
        fft_symbols = dlx_dl._new_file_symbols(date_from, date_to)
    
        if len(fft_symbols) > 10000:
            raise Exception('that\'s too many file symbols to look up, sorry :(')

        print(f'found files for {len(fft_symbols)} symbols')
    
    criteria = SON({'$gte': date_from})
    
    if date_to:
        criteria['$lte'] = date_to

    query = {'$or': [{'updated': criteria}, {'191.subfields.value': {'$in': fft_symbols}}]} if cls == BibSet \
        else {'updated': criteria}
        
    rset = cls.from_query(query)

    return rset
    
    hist = DB.handle['bib_history'] if cls == BibSet else DB.handle['auth_history']
    deleted = list(hist.find({'deleted.time': {'$gte': date_from}}))

    if deleted:
        if delete_only:
            rset.records = []

        rcls = Bib if cls == BibSet else Auth
        records = list(rset.records)
        to_delete = []
        
        for d in deleted:
            r = rcls({'_id': d['_id']})
            r.set('980', 'a', 'DELETED')
            r.updated = d['deleted']['time']
            to_delete.append(r)

        rset.records = (r for r in records + to_delete) # program is expecting an iterable
    
    return rset

def clean_values(record):
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
    
    return record

def export_whole_record(args, record, *, export_type):
    if export_type not in ['NEW', 'UPDATE', 'DELETE']:
        raise Exception('invalid "export_type"')

    # perform necessary transformations
    record = clean_values(record)

    if args.type == 'bib':
        record = dlx_dl.process_bib(record, blacklisted=args.blacklisted, files_only=False)
    else:
        record = dlx_dl.process_auth(record)

    return submit_to_dl(args, record, mode='insertorreplace', export_start=args.START, export_type=export_type)

def compare_and_update(args, *, dlx_record, dl_record):
    dlx_record = clean_values(dlx_record)

    # values from dlx not in dl
    take_tags = set()
    
    for field in dlx_record.fields:
        taken = {}

        # skip fields
        if re.match('^00', field.tag):
            continue
        elif field.tag in ('035', '856', '949', '980', '998'):
            continue
        
        # scan subfield values
        for subfield in field.subfields:
            if field.tag == '191' and subfield.code in ('q', 'r'):
                continue
            elif subfield.value in ('', None):
                continue
            
            # filter out fields with same tag/indicator combo
            values = []
            field.ind1 = ' ' if field.ind1 == '_' else field.ind1
            field.ind2 = ' ' if field.ind2 == '_' else field.ind2

            for f in filter(lambda x: x.tag == field.tag, dl_record.datafields):
                if f.indicators == field.indicators: 
                    for s in f.subfields:
                        values.append(s.value)

            # ignore unicode differences for now
            check_values = unicodedata.normalize('NFD', subfield.value) in [unicodedata.normalize('NFD', x) for x in values]
            check_tags = field.tag + ''.join(field.indicators) in [x.tag + ''.join(x.indicators) for x in dl_record.datafields]

            if not check_values or not check_tags:
                print(f'{dlx_record.id}: {field.tag}  {field.indicators} ${subfield.code}: {subfield.value} X {dl_record.get_values(field.tag, subfield.code)}')
                take_tags.add(field.tag)
                taken[field.tag] = True
                break
            
        if taken.get(field.tag):
            continue

    # tag/indicator combo from dl not in dlX
    delete_fields = []

    for field in dl_record.fields:
        deleted = {}

        # skip fields
        if re.match('^00', field.tag):
            continue
        elif field.tag in ('035', '856', '949', '980'):
            continue

        if field.tag + ''.join(field.indicators) not in [x.tag + ''.join(x.indicators) for x in dlx_record.datafields]:
            print(str(dl_record.id) + ' TO DELETE: ' + field.to_mrk())
            
            # delete field by setting all values to empty string
            for s in field.subfields:
                s.value = ''

            delete_fields.append(field)

    # duplicated fields
    seen = []
    
    for field in filter(lambda x: x.get_value('0') not in ('', 'BAD XREF'), dl_record.datafields):
        if field.to_mrk() in seen:
            # check if field is also duplicated in dlx
            if len(dlx_record.get_fields(field.tag)) != len(dl_record.get_fields(field.tag)):
                print(f'{dlx_record.id}: DUPLICATED FIELD: ' + field.to_mrk())
                take_tags.add(field.tag)

        seen.append(field.to_mrk())

    # collector tool files
    for field in dlx_record.get_fields('856'):
        if field.get_value('3') == 'Thumbnail':
            continue
        
        url = field.get_value('u')

        if urlparse(url).netloc not in dlx_dl.WHITELIST:
            for s in field.subfields:
                if s.value not in dl_record.get_values('856', s.code):
                    take_tags.add('856')
        else:
            # from Collector Tool
            if len(list(filter(lambda x: 'digitallibrary.un.org' in x, dl_record.get_values('856', 'u')))) == 0:
                print(f'{dlx_record.id}: FILE NOT FOUND ' + url)
                
                return export_whole_record(args, dlx_record, export_type='UPDATE')

            fn = url.split('/')[-1]
            fn = dlx_dl.clean_fn(fn)

            # chars requiring encoding
            fn = fn.replace('%', '%25')
            #fn = fn.replace('^', '%5E')
            #fn = quote(fn)

            if unquote(fn) == fn:
                fn = quote(fn)

            dl_vals = [x.split('/')[-1] for x in dl_record.get_values('856', 'u')]

            # remove extra chars if any
            try:
                dl_vals = [x[:len(fn)-fn[::-1].index('.')-1] + fn[-fn[::-1].index('.')-1:len(fn)] for x in dl_vals]
            except ValueError:
                pass
            except Exception as e:
                print(f'Error: {dlx_record.id}')
                raise e

            if fn not in dl_vals:
                print(f'{dlx_record.id}: FILE NOT FOUND ' + url)

                return export_whole_record(args, dlx_record, export_type='UPDATE')

    # official doc files
    #symbols = dlx_record.get_values('191', 'a') + dlx_record.get_values('191', 'z') if args.type == 'bib' else []
    symbols = dlx_record.get_values('191', 'a') if args.type == 'bib' else []
    
    for symbol in set(symbols):
        if symbol == '' or symbol == ' ' or symbol == '***': # note: clean these up in db
            continue
           
        for lang in ('AR', 'ZH', 'EN', 'FR', 'RU', 'ES', 'DE'):
            if f := File.latest_by_identifier_language(Identifier('symbol', symbol), lang):
                field = next(filter(lambda x: re.search(f'{lang}\.\w+$', x.get_value('u')), dl_record.get_fields('856')), None)
                
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


    # request params
    headers = {'Authorization': 'Token ' + args.api_key, 'Content-Type': 'application/xml; charset=utf-8'}
    nonce = {'type': args.type, 'id': dlx_record.id, 'key': args.nonce_key}
    params = {'mode': 'correct', 'callback_url': args.callback_url, 'nonce': json.dumps(nonce)}
    
    # run api submission
    if take_tags or delete_fields:
        record = Bib() if args.type == 'bib' else Auth()
        record.id = dlx_record.id
        record.set('001', None, dl_record.get_value('001'))
        
        for tag in sorted(list(take_tags)):
            record.fields += dlx_record.get_fields(tag)

        record.fields += delete_fields
        record.fields.append(dlx_record.get_field('998'))

        return submit_to_dl(args, record, mode='correct', export_start=args.START, export_type='UPDATE')

    return

def submit_to_dl(args, record, *, mode, export_start, export_type):
    if mode not in ('insertorreplace', 'correct'):
        raise Exception('invalid "mode"')

    if export_type not in ('NEW', 'UPDATE', 'DELETE'):
        raise Exception('invalid "export_type"')

    xml = record.to_xml(xref_prefix='(DHLAUTH)')
    
    headers = {
        'Authorization': 'Token ' + args.api_key,
        'Content-Type': 'application/xml; charset=utf-8',
    }

    nonce = {'type': args.type, 'id': record.id, 'key': args.nonce_key}
    
    params = {
        'mode': mode,
        'callback_url': args.callback_url,
        'nonce': json.dumps(nonce)
    } 

    response = requests.post(API_RECORD_URL, params=params, headers=headers, data=xml.encode('utf-8'))
    
    logdata = {
        'export_start': export_start,
        'export_type': export_type,
        'time': datetime.now(timezone.utc),
        'source': args.source,
        'record_type': args.type, 
        'record_id': record.id, 
        'response_code': response.status_code, 
        'response_text': response.text.replace('\n', ''),
        'xml': xml
    }

    args.log.insert_one(logdata)
    logdata['export_start'] = logdata['export_start'].isoformat()
    logdata['time'] = logdata['time'].isoformat()
    print(logdata)

    return logdata

if __name__ == '__main__':
    run()