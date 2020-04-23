import os, sys, re, requests, json
from warnings import warn
from urllib.parse import urlparse, urlunparse, quote
from datetime import datetime, timezone, timedelta
from argparse import ArgumentParser
from dlx import DB, Config
from dlx.marc import Bib, BibSet, Auth, AuthSet
from pymongo import MongoClient
from mongomock import MongoClient as MockClient

###

parser = ArgumentParser(prog='dlx-dl')
parser.add_argument('--connect', required=True, help='dlx MDB connection string')
parser.add_argument('--type', required=True, choices=['bib', 'auth'])
parser.add_argument('--modified_from', help='ISO datetime (UTC)')
parser.add_argument('--modified_to', help='ISO datetime (UTC)')
parser.add_argument('--modified_within', help='Seconds')
parser.add_argument('--modified_since_log', action='store_true', help='boolean')
parser.add_argument('--list', help='file with list of IDs (max 1000)')
parser.add_argument('--id', help='a single record ID')
parser.add_argument('--output_file', help='write XML as batch to this file')
parser.add_argument('--api_key', help='UNDL-issued api key')
parser.add_argument('--email', help='disabled')
parser.add_argument('--log', help='log MDB connection string')
parser.add_argument('--preview', action='store_true', help='list records that meet criteria and exit (boolean)')

###

API_URL = 'https://digitallibrary.un.org/api/v1/record/'
EXPORT_TIME = datetime.now(timezone.utc)
LOG_COLLECTION_NAME = 'dlx_dl_log'
LOG_DATA = []
WHITELIST = ['digitization.s3.amazonaws.com', 'undl-js.s3.amazonaws.com', 'un-maps.s3.amazonaws.com', 'dag.un.org']

###

def main(**kwargs):
    if kwargs:
        sys.argv[1:] = ['--{}={}'.format(key, val) for key, val in kwargs.items()]
    
    args = parser.parse_args()
    DB.connect(args.connect)
    args.email = None
    
    ## process arguments
    
    if args.api_key and args.log:
        cstr = args.log
        
        if cstr[0:9] == 'mongomock':
            cstr = 'mongodb://.../?authSource=dlx_dl_dummy'
            log = MockClient(cstr)['dlx_dl_dummy'][LOG_COLLECTION_NAME]
        else:
            match = re.search(r'\?authSource=([\w]+)', cstr)

            if match:
                log_db_name = match.group(1)
            else:
                raise Exception('Log DB name not found')

            log = MongoClient(cstr)[log_db_name][LOG_COLLECTION_NAME]
    else:
        log = None
    
    cls = BibSet if args.type == 'bib' else AuthSet

    if args.id:
        rset = cls.from_query({'_id': int(args.id)})
    elif args.modified_since_log:
        last_export = next(log.aggregate([{'$sort': {'export_start' : -1}}]))['export_start']
        rset = cls.from_query({'updated': {'$gte': last_export}})
    elif args.modified_within:
        rset = cls.from_query({'updated': {'$gte': datetime.utcnow() - timedelta(seconds=int(args.modified_within))}})
    elif args.modified_from and args.modified_to:
        rset = cls.from_query({'updated': {'$gte': datetime.fromisoformat(args.modified_from), '$lt': datetime.fromisoformat(args.modified_to)}})
    elif args.modified_from:
        rset = cls.from_query({'updated': {'$gte': datetime.fromisoformat(args.modified_from)}})
    elif args.list:
        with open(args.list, 'r') as f:
            ids = [int(line) for line in f.readlines()]
            
            if len(ids) > 1000:
                raise Exception('Max 1000 IDs')
                
            rset = cls.from_query({'_id': {'$in': ids}})
    else:
        raise Exception('One of the arguments --id --modified_from --modified_within --list is required')
        
    if args.preview:
        for record in rset:
            print('\t'.join([str(record.id), str(record.updated)]))

        return

    if args.output_file:
        if args.output_file.lower() == 'stdout':
            out = sys.stdout
        else:
            out = open(args.output_file, 'w', encoding='utf-8')
    else:
        out = open(os.devnull, 'w')

    ## write
        
    out.write('<collection>')
    
    if args.type == 'bib':
        process_bibs(rset, out, args.api_key, args.email, log)
    else:
        process_auths(rset, out, args.api_key, args.email, log)
    
    out.write('</collection>')
    
    return
    
###

def process_bibs(rset, out, api_key, email, log):
    for bib in rset:
        bib.delete_field('001')
        bib.delete_field('005')
        bib = _035(bib)
        
        place = 0
        
        for field in bib.get_fields('856'):
            url = field.get_value('u')
            parsed = urlparse(url)
            
            if parsed.netloc in WHITELIST:
                bib.set('FFT', 'a', urlunparse([parsed.scheme, parsed.netloc, quote(parsed.path), None, None, None]), address=['+'])
                old_fn = url.split('/')[-1] 
                new_fn = clean_fn(old_fn)
                bib.set('FFT', 'n', new_fn, address=[place])
                
                if parsed.path.split('.')[-1] == 'tiff':
                    bib.set('FFT', 'r', 'tiff', address=[place])
                    
                lang = field.get_value('3')
                
                if lang:
                    lang = 'English' if lang == 'Eng' else lang
                    bib.set('FFT', 'd', lang, address=[place])
                    
                fmt = field.get_value('q')
                
                if fmt:
                    bib.set('FFT', 'f', fmt, address=[place])
                
                bib.fields.remove(field)
                
                place += 1

        bib.set('980', 'a', 'BIB')
        
        xml = bib.to_xml(xref_prefix='(DHLAUTH)')
        
        out.write(xml)
        
        if api_key:
            post('bib', bib.id, xml, api_key, email, log)
           
def process_auths(rset, out, api_key, email, log):
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
    
    for auth in rset:
        atag = auth.heading_field.tag
        
        if atag == '650':
            warn('Can\'t update thesaurus terms at this time: record ' + auth.id)
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
            post('auth', auth.id, xml, api_key, email, log)
            
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
    
def clean_fn(fn):
    parts = fn.split('.')
    fn = '-'.join(parts[:-1]) + '.' + parts[-1]
    fn = fn.translate(str.maketrans(' [];', '_^^&'))
    return fn
  
def post(rtype, rid, xml, api_key, email, log):
    headers = {
        'Authorization': 'Token ' + api_key,
        'Content-Type': 'application/xml; charset=utf-8',
        
    }
    
    params = {
        'mode': 'insertorreplace',
        'callback_email': email
    }

    response = requests.post(API_URL, params=params, headers=headers, data=xml.encode('utf-8'))
     
    logdata = {
        'export_start': EXPORT_TIME,
        'time': datetime.now(timezone.utc),
        'record_type': rtype, 
        'record_id': rid, 
        'response_code': response.status_code, 
        'response_text': response.text.replace('\n', ''),
        'xml': xml
    }
    
    if log:
        log.insert_one(logdata)
    
    # clean for JSON serialization
    logdata.pop('_id', None) # pymongo adds the _id key to the dict on insert??
    logdata['export_start'] = str(logdata['export_start'])
    logdata['time'] = str(logdata['time'])
    
    LOG_DATA.append(logdata)
    
    print(json.dumps(logdata))

###

if __name__ == '__main__':
    main()

