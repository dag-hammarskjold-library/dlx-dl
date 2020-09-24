import os, sys, re, requests, json
from warnings import warn
from urllib.parse import urlparse, urlunparse, quote, unquote
from datetime import datetime, timezone, timedelta
from argparse import ArgumentParser
from dlx import DB, Config
from dlx.marc import Bib, BibSet, Auth, AuthSet, Datafield
#from dlx.file import File
from pymongo import MongoClient
from mongomock import MongoClient as MockClient
from bson import SON

###

parser = ArgumentParser(prog='dlx-dl')
parser.add_argument('--connect', required=True, help='dlx MDB connection string')
parser.add_argument('--type', required=True, choices=['bib', 'auth'])
parser.add_argument('--modified_from', help='ISO datetime (UTC)')
parser.add_argument('--modified_to', help='ISO datetime (UTC)')
parser.add_argument('--modified_within', help='Seconds')
parser.add_argument('--modified_since_log', action='store_true', help='boolean')
parser.add_argument('--list', help='file with list of IDs (max 5000)')
parser.add_argument('--id', help='a single record ID')
parser.add_argument('--output_file', help='write XML as batch to this file. use "STDOUT" to print in console')
parser.add_argument('--api_key', help='UNDL-issued api key')
parser.add_argument('--email', help='disabled')
parser.add_argument('--callback_url', help="A URL that can receive the results of a submitted task.")
parser.add_argument('--nonce_key', help='A validation key that will be passed to and from the UNDL API.')
parser.add_argument('--files_only', action='store_true', help='only export records with new files')
parser.add_argument('--preview', action='store_true', help='list records that meet criteria and exit (boolean)')

###

API_URL = 'https://digitallibrary.un.org/api/v1/record/'
LOG_COLLECTION = 'dlx_dl_log'
BLACKLIST_COLLECTION = 'blacklist'
WHITELIST = ['digitization.s3.amazonaws.com', 'undl-js.s3.amazonaws.com', 'un-maps.s3.amazonaws.com', 'dag.un.org']

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

def run(**kwargs):
    if kwargs:
        sys.argv[1:] = ['--{}={}'.format(key, val) for key, val in kwargs.items()]
    
    args = parser.parse_args()
    
    if isinstance(kwargs.get('connect'), (MongoClient, MockClient)):
        DB.client = kwargs['connect']
    else:
        DB.connect(args.connect)

    #args.email = None

    ## process arguments
    
    log = DB.handle[LOG_COLLECTION]
    blacklist = DB.handle[BLACKLIST_COLLECTION]
    blacklisted = [x['symbol'] for x in blacklist.find({})]
    
    cls = BibSet if args.type == 'bib' else AuthSet
    since, to = None, None

    if args.modified_within:
        since = datetime.utcnow() - timedelta(seconds=int(args.modified_within))
        rset = _get_recordset(cls, since)
    elif args.modified_from and args.modified_to:
        since = datetime.fromisoformat(args.modified_from)
        to = datetime.fromisoformat(args.modified_to)
        rset = _get_recordset(cls, since, to)
    elif args.modified_from:
        since = datetime.fromisoformat(args.modified_from)
        rset = _get_recordset(cls, since)
    elif args.modified_since_log:
        last_export = next(log.aggregate([{'$sort': {'export_start' : -1}}]))['export_start']
        rset = _get_recordset(cls, last_export)
    elif args.id:
        rset = cls.from_query({'_id': int(args.id)})
    elif args.list:
        with open(args.list, 'r') as f:
            ids = [int(row[0]) for row in [line.split("\t") for line in f.readlines()]]
            
            if len(ids) > 5000:
                raise Exception('Max 5000 IDs')
                
            rset = cls.from_query({'_id': {'$in': ids}})
    else:
        raise Exception('One of the arguments --id --modified_from --modified_within --list is required')
        
    if args.preview:
        for record in rset:
            denote = ''
            
            if to and record.updated > to:
                # the record has been updated since the file
                denote = '*'
            elif since and record.updated < since:
                # the file has been updated since the record
                denote = '**'

            print('\t'.join([str(record.id), str(record.updated), denote]))

        return

    if args.output_file:
        if args.output_file.lower() == 'stdout':
            if args.api_key:
                raise Exception('Can\'t set --output_file to STDOUT with --api_key')
                
            out = sys.stdout
        else:
            out = open(args.output_file, 'w', encoding='utf-8')
    else:
        out = open(os.devnull, 'w')

    ## write
    
    if args.type == 'bib':
        process_bibs(rset, out, args.api_key, args.email, args.callback_url, args.nonce_key, log, args.files_only, blacklisted)
    elif args.type == 'auth':
        process_auths(rset, out, args.api_key, args.email, args.callback_url, args.nonce_key, log)
    
    return
    
###

def process_bibs(rset, out, api_key, email, callback_url, nonce_key, log, files_only, blacklisted):
    export_start = datetime.now(timezone.utc)
    
    out.write('<collection>')
    
    for bib in rset:
        if bib.get_value('191', 'a') not in blacklisted:
            _fft_from_files(bib)
        
        if files_only and not bib.get_fields('FFT'):
            continue
        
        bib.delete_field('001')
        bib.delete_field('005')
        bib = _035(bib)
        bib = _856(bib)
        bib.set('980', 'a', 'BIB')

        xml = bib.to_xml(xref_prefix='(DHLAUTH)')
        
        out.write(xml)
        
        if api_key:
            post('bib', bib.id, xml, api_key, email, callback_url, nonce_key, log, export_start)
    
    out.write('</collection>')
    
def process_auths(rset, out, api_key, email, callback_url, nonce_key, log):
    export_start = datetime.now(timezone.utc)
    
    out.write('<collection>')
    
    for auth in rset:
        atag = auth.heading_field.tag
        
        if atag == '150':
            warn('Can\'t update thesaurus terms at this time: record {}'.format(auth.id))
            continue
    
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

        xml = auth.to_xml(xref_prefix='(DHLAUTH)')
        
        out.write(xml)
                    
        if api_key:
            post('auth', auth.id, xml, api_key, email, callback_url, nonce_key, log, export_start)
            
    out.write('</collection>')
    
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

def _get_recordset(cls, date_from, date_to=None):
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

    for f in DB.files.find('$or': [{'timestamp': criteria}, {'updated': criteria}]):
        for idx in f['identifiers']:
            if idx['type'] == 'symbol' and idx['value'] != '' and idx['value'] != ' ' and idx['value'] != '***': # note: clean these up in db
                fft_symbols.append(idx['value'])
                
    return list(set(fft_symbols))
    
def _fft_from_files(bib):
    symbols = bib.get_values('191', 'a')
    
    for symbol in symbols:
        if symbol == '' or symbol == ' ' or symbol == '***': # note: clean these up in db
            continue
        
        files = DB.files.find({'identifiers': {'type': 'symbol', 'value': symbol}}, projection={'uri': 1, 'languages': 1, 'timestamp': 1})
        
        latest_lang = {}
        
        for f in files:
            lang = f['languages'][0]
            
            if lang not in latest_lang:
                latest_lang[lang] = f['_id']
            else:
                if f['timestamp'] > DB.files.find_one({'_id': latest_lang[lang]}, projection={'timestamp': 1})['timestamp']:
                    latest_lang[lang] = f['_id']
                    
        for lang, idx in latest_lang.items():
            f = DB.files.find_one({'_id': idx})
            
            field = Datafield(record_type='bib', tag='FFT', ind1=' ', ind2=' ')
            field.set('a', 'https://' + f['uri'])
            
            try:
                field.set('d', ISO_STR[lang])
            except:
                raise Exception(lang)
                
            field.set('n', encode_fn(symbols, lang, 'pdf'))
            
            bib.fields.append(field)
        
        return bib
    
def clean_fn(fn):
    parts = fn.split('.')
    fn = '-'.join(parts[:-1]) + '.' + parts[-1]
    fn = fn.translate(str.maketrans(' [];', '_^^&'))
    return fn
    
def encode_fn(symbols, language, extension):
    from dlx.util import ISO6391
    
    ISO6391.codes[language.lower()]
    symbols = [symbols] if isinstance(symbols, str) else symbols
    xsymbols = [sym.translate(str.maketrans(' /[]*:;', '__^^!#%')) for sym in symbols]

    return '{}-{}.{}'.format('&'.join(xsymbols), language.upper(), extension)
  
def post(rtype, rid, xml, api_key, email, callback_url, nonce_key, log, started_at):
    headers = {
        'Authorization': 'Token ' + api_key,
        'Content-Type': 'application/xml; charset=utf-8',
    }

    nonce = {
        'type': rtype,
        'id': rid,
        'key': nonce_key
    }
    
    params = {
        'mode': 'insertorreplace',
        'callback_email': email,
        'callback_url': callback_url,
        'nonce': json.dumps(nonce)
    }

    response = requests.post(API_URL, params=params, headers=headers, data=xml.encode('utf-8'))
     
    logdata = {
        'export_start': started_at,
        'time': datetime.now(timezone.utc),
        'record_type': rtype, 
        'record_id': rid, 
        'response_code': response.status_code, 
        'response_text': response.text.replace('\n', ''),
        'xml': xml
    }
    
    log.insert_one(logdata)
    
    # clean for JSON serialization
    logdata.pop('_id', None) # pymongo adds the _id key to the dict on insert??
    logdata['export_start'] = str(logdata['export_start'])
    logdata['time'] = str(logdata['time'])
    
    print(json.dumps(logdata))

###

if __name__ == '__main__':
    run()
