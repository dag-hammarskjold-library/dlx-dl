import os, sys, math, re, requests, json
from warnings import warn
from urllib.parse import urlparse, urlunparse, quote, unquote
from datetime import datetime, timezone, timedelta
from argparse import ArgumentParser
from dlx import DB, Config
from dlx.marc import Bib, BibSet, Auth, AuthSet, Datafield
from dlx.file import File, Identifier
from pymongo import MongoClient, DESCENDING
from mongomock import MongoClient as MockClient
from bson import SON

###

parser = ArgumentParser(prog='dlx-dl')
parser.add_argument('--connect', required=True, help='dlx MDB connection string')
parser.add_argument('--source', help='an identity to use in the log')
parser.add_argument('--type', required=True, choices=['bib', 'auth'])
parser.add_argument('--modified_from', help='ISO datetime (UTC)')
parser.add_argument('--modified_to', help='ISO datetime (UTC)')
parser.add_argument('--modified_within', help='Seconds')
parser.add_argument('--modified_since_log', action='store_true', help='boolean')
parser.add_argument('--list', help='file with list of IDs (max 5000)')
parser.add_argument('--id', help='a single record ID')
parser.add_argument('--ids', nargs='+', help='variable-length list of record IDs')
parser.add_argument('--output_file', help='write XML as batch to this file. use "STDOUT" to print in console')
parser.add_argument('--api_key', help='UNDL-issued api key')
parser.add_argument('--email', help='disabled')
parser.add_argument('--callback_url', help="A URL that can receive the results of a submitted task.")
parser.add_argument('--nonce_key', help='A validation key that will be passed to and from the UNDL API.')
parser.add_argument('--files_only', action='store_true', help='only export records with new files')
parser.add_argument('--preview', action='store_true', help='list records that meet criteria and exit (boolean)')
parser.add_argument('--queue', help='Number of records at which to limit export and queue')

###

API_URL = 'https://digitallibrary.un.org/api/v1/record/'
LOG_COLLECTION = 'dlx_dl_log'
QUEUE_COLLECTION = 'dlx_dl_queue'
BLACKLIST_COLLECTION = 'blacklist'
WHITELIST = ['digitization.s3.amazonaws.com', 'undl-js.s3.amazonaws.com', 'un-maps.s3.amazonaws.com', 'dag.un.org']
LIMIT = math.inf

AUTH_TYPE = {
    '100': 'PERSONAL',
    '110': 'CORPORATE',
    '111': 'MEETING',
    '130': 'UNIFORM',
    '150': 'TOPICAL',
    '151': 'GEOGRAPHIC',
    '190': 'SYMBOL',
    '191': 'AGENDA'
}

ISO_STR = {
    'AR': 'العربية',
	'ZH': '中文',
	'EN': 'English',
	'FR': 'Français',
	'RU': 'Русский',
	'ES': 'Español',
	#'DE': 'Deutsch',
	'DE': 'Other',
}

###

def get_args(**kwargs):
    if kwargs:
        ids, since_log, fonly, preview = [kwargs.get(x) and kwargs.pop(x) for x in ('ids', 'modified_since_log', 'files_only', 'preview')]
        
        sys.argv[1:] = ['--{}={}'.format(key, val) for key, val in kwargs.items()]
        
        if fonly: sys.argv.append('--files_only')
        if preview: sys.argv.append('--preview')
        if since_log: sys.argv.append('--modified_since_log')
        if ids:
            sys.argv.append('--ids')
            sys.argv += ids
     
    return parser.parse_args()

def run(**kwargs):
    args = get_args(**kwargs)
    
    ### connect to DB
    
    if isinstance(kwargs.get('connect'), (MongoClient, MockClient)):
        # for testing 
        DB.client = kwargs['connect']
    else:
        DB.connect(args.connect)

    log = DB.handle[LOG_COLLECTION]
    queue = DB.handle[QUEUE_COLLECTION]
    blacklist = DB.handle[BLACKLIST_COLLECTION]
    blacklisted = [x['symbol'] for x in blacklist.find({})]
    
    ### criteria
    
    records = get_records(args, log, queue)
        
    ### write
    
    out = output_handle(args)
    export_start = datetime.now(timezone.utc)
    seen = []
    
    out.write('<collection>')
    
    for record in records:
        if record.id in seen:
            continue

        if args.type == 'bib':
            if record.get_value('245', 'a')[0:16].lower() == 'work in progress':
                continue
                
            record = process_bib(record, blacklisted=blacklisted, files_only=args.files_only)
        elif args.type == 'auth':
            record = process_auth(record)
            
        # values of "-" cause DL import error
        for field in record.datafields:
            for sub in field.subfields:
                if not hasattr(sub, 'xref') and sub.value == '-':
                    sub.value = '_'
        
        xml = record.to_xml(xref_prefix='(DHLAUTH)')
        
        if args.api_key:
            logdata = submit_to_dl(record, export_start, args)
            queue.delete_many({'type': args.type, 'record_id': record.id})     
            log.insert_one(logdata)
            
            # clean for JSON serialization
            logdata.pop('_id', None) # pymongo adds the _id key to the dict on insert??
            logdata['export_start'] = str(logdata['export_start'])
            logdata['time'] = str(logdata['time'])
            print(json.dumps(logdata))
            
            queue.delete_many({'type': args.type, 'record_id': record.id})
        
        seen.append(record.id)
        out.write(xml)

    out.write('</collection>')
    
    if args.api_key:    
        log.insert_one({'source': args.source, 'record_type': args.type, 'export_start': export_start, 'export_end': datetime.now(timezone.utc)})
    
    return
    
###

def get_records(args, log, queue):
    cls = BibSet if args.type == 'bib' else AuthSet
    since, to = None, None

    if args.modified_within:
        since = datetime.utcnow() - timedelta(seconds=int(args.modified_within))
        records = get_records_by_date(cls, since)
    elif args.modified_from and args.modified_to:
        since = datetime.strptime(args.modified_from, '%Y-%m-%d')
        to = datetime.strptime(args.modified_to, '%Y-%m-%d')
        records = get_records_by_date(cls, since, to)
    elif args.modified_from:
        since = datetime.strptime(args.modified_from, '%Y-%m-%d')
        records = get_records_by_date(cls, since)
    elif args.modified_since_log:
        c = log.find({'source': args.source, 'record_type': args.type, 'export_end': {'$exists': 1}}, sort=[('export_start', DESCENDING)], limit=1)
        last = next(c, None)
        if last:
            last_export = last['export_start']
            records = get_records_by_date(cls, last_export)
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
    else:
        raise Exception('One of the arguments --id --modified_from --modified_within --list is required')
        
    ### preview
    
    if args.preview:
        print(preview(records, since, to))
        exit()

    ### queue
    
    to_process = []
    limit = int(args.queue or 0) or LIMIT

    for i, r in enumerate(records):
        if i < limit:
            to_process.append(r)
        else:
            if i == limit:
                warn(f'Limiting export set to {limit} and adding the rest to the queue')
            
            queue.insert_one(
                {'time': datetime.now(timezone.utc), 'source': args.source, 'type': args.type, 'record_id': r.id}
            )
    
    if args.queue is not None and len(to_process) < limit:
        free_space = limit - len(to_process)
        queued = queue.find({'source': args.source, 'type': args.type}, limit=free_space)
        
        for i, d in enumerate(queued):
            to_process.append(next(cls.from_query({'_id': d['record_id']})))
            
        if i:
            warn(f'Took {i} from queue')

    return to_process

def preview(records, since, to):
    for record in records:
        denote = ''
        
        if to and record.updated > to:
            # the record has been updated since the file
            denote = '*'
        elif since and record.updated < since:
            # the file has been updated since the record
            denote = '**'
            
        return '\t'.join([str(record.id), str(record.updated), denote])

def output_handle(args):
    if args.output_file:
        if args.output_file.lower() == 'stdout':
            if args.api_key:
                warn('Can\'t set --output_file to STDOUT with --api_key')
                out = open(os.devnull, 'w', encoding='utf-8')
            else:
                out = sys.stdout
        else:
            out = open(args.output_file, 'w', encoding='utf-8')
    else:
        out = open(os.devnull, 'w', encoding='utf-8')
        
    return out

def process_bib(bib, *, blacklisted, files_only):
    if bib.get_value('245', 'a')[0:16].lower() == 'work in progress':
        return bib
        
    flag = False
        
    for sym in bib.get_values('191', 'a'):
        if sym in blacklisted:
            flag = True

    if not flag:
        _fft_from_files(bib)
    
    if files_only and not bib.get_fields('FFT'):
        return bib
    
    bib.delete_field('001')
    bib.delete_field('005')
    bib = _035(bib)
    bib = _856(bib)
    bib.set('980', 'a', 'BIB')
    
    return bib
    
def process_auth(auth):
    atag = auth.heading_field.tag
        
    auth.delete_field('001')
    auth.delete_field('005')
    auth = _035(auth)
    auth.set('980', 'a', 'AUTHORITY')
        
    if atag in AUTH_TYPE.keys():
        atype = AUTH_TYPE[atag]
        auth.set('980', 'a', atype, address=['+'])
            
        if atag == '110':
            if auth.heading_field.get_value('9') == 'ms':
                auth.set('980', 'a', 'MEMBER', address=['+'])
                
    return auth
    
def _035(record):
    place = 0
    
    for field in record.get_fields('035'):
        ctr = field.get_value('a')
        pre = ctr[0]
        new = str(record.id) + 'X'
        
        if re.match(r'[A-Z]', pre):
            new = pre + new
        
        record.set('035', 'a', new, address=[place])
        record.set('035', 'z', ctr, address=[place])
        
        place += 1
    
    pre = '(DHL)' if type(record) == Bib else '(DHLAUTH)'
    record.set('035', 'a', pre + str(record.id), address=['+'])
    
    return record
    
def _856(bib):
    place = len(bib.get_fields('FFT'))
    seen = []
    
    for field in bib.get_fields('856'):
        url = field.get_value('u')
        parsed = urlparse(url)
        
        if parsed.netloc in WHITELIST:
            url_path = parsed.path.rstrip()
            
            if unquote(url_path) == url_path:
                url_path = quote(url_path)
            
            bib.set('FFT', 'a', urlunparse([parsed.scheme, parsed.netloc, url_path, None, None, None]), address=['+'])
            old_fn = url.split('/')[-1]
            new_fn = clean_fn(old_fn)
            parts = new_fn.split('.')
            base = ''.join(parts[0:-1])

            if base in seen:
                ext = parts[-1]
                new_fn = f'{base}_{place}.{ext}'
            else:
                seen.append(base)
            
            bib.set('FFT', 'n', new_fn, address=[place])

            if parsed.path.split('.')[-1] == 'tiff':
                bib.set('FFT', 'r', 'tiff', address=[place])
                
            lang = field.get_value('3')
            
            if lang:
                lang = 'English' if lang == 'Eng' else lang
                bib.set('FFT', 'd', lang, address=[place])
            
            bib.fields.remove(field)
            place += 1
            
    return bib

def get_records_by_date(cls, date_from, date_to=None):
    fft_symbols = _new_file_symbols(date_from, date_to)
    
    if len(fft_symbols) > 1000:
        raise Exception('that\'s too many file symbols to look up, sorry :(')
    
    criteria = SON({'$gte': date_from})
    
    if date_to:
        criteria['$lte'] = date_to
        
    rset = cls.from_query(
        {
            '$or': [
                {'updated': criteria},
                {'191.subfields.value': {'$in': fft_symbols}}
            ]
        }
    )
    
    return rset
    
def _new_file_symbols(date_from, date_to=None):
    fft_symbols = []
    criteria = {'$gte': date_from}
    date_to and criteria.setdefault('$lte', date_to)

    for f in DB.files.find({'$or': [{'timestamp': criteria}, {'updated': criteria}]}):
        for idx in f['identifiers']:
            if idx['type'] == 'symbol' and idx['value'] != '' and idx['value'] != ' ' and idx['value'] != '***': # note: clean these up in db
                fft_symbols.append(idx['value'])
                
    return list(set(fft_symbols))
    
def _fft_from_files(bib):
    symbols = bib.get_values('191', 'a')
    
    seen = []
    
    for symbol in set(symbols):
        if symbol == '' or symbol == ' ' or symbol == '***': # note: clean these up in db
            continue
           
        for lang in ('AR', 'ZH', 'EN', 'FR', 'RU', 'ES', 'DE'):
            xfile = File.latest_by_identifier_language(Identifier('symbol', symbol), lang)
            
            if xfile and xfile.uri not in seen:
                field = Datafield(record_type='bib', tag='FFT', ind1=' ', ind2=' ')
                field.set('a', 'https://' + xfile.uri)
                field.set('d', ISO_STR[lang])
                field.set('n', encode_fn(symbols, lang, 'pdf'))
                bib.fields.append(field)
                
                seen.append(xfile.uri)
        
    return bib
    
def clean_fn(fn):
    parts = fn.split('.')
    fn = '-'.join(parts[:-1]) + '.' + parts[-1]
    fn = fn.translate(str.maketrans(' [];', '_^^!'))
    return fn
    
def encode_fn(symbols, language, extension):
    from dlx.util import ISO6391
    
    ISO6391.codes[language.lower()]
    symbols = [symbols] if isinstance(symbols, str) else symbols
    xsymbols = [sym.translate(str.maketrans(' /[]*:;', '__^^!#%')) for sym in symbols]

    return '{}-{}.{}'.format('--'.join(xsymbols), language.upper(), extension)

def submit_to_dl(record, export_start, args):
    xml = record.to_xml(xref_prefix='(DHLAUTH)')
    
    headers = {
                'Authorization': 'Token ' + args.api_key,
        'Content-Type': 'application/xml; charset=utf-8',
    }

    nonce = {'type': args.type, 'id': record.id, 'key': args.nonce_key}
    
    params = {
        'mode': 'insertorreplace',
        'callback_email': args.email,
        'callback_url': args.callback_url,
        'nonce': json.dumps(nonce)
    }

    response = requests.post(API_URL, params=params, headers=headers, data=xml.encode('utf-8'))
    
    logdata = {
        'export_start': export_start,
        'time': datetime.now(timezone.utc),
        'source': args.source,
        'record_type': args.type, 
        'record_id': record.id, 
        'response_code': response.status_code, 
        'response_text': response.text.replace('\n', ''),
        'xml': xml
    }
    
    return logdata

###

if __name__ == '__main__':
    run()
