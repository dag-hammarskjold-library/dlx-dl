import os, sys, re, requests, json
from warnings import warn
from datetime import datetime, timezone
from argparse import ArgumentParser
from dlx import DB, Config
from dlx.marc import Bib, BibSet, Auth, AuthSet
from pymongo import MongoClient
from mongomock import MongoClient as MockClient

###

parser = ArgumentParser(prog='dlx-dl')
parser.add_argument('--connect', required=True, help='MDB connection string')
parser.add_argument('--type', required=True, choices=['bib', 'auth'])
parser.add_argument('--modified_from', help='ISO datetime (UTC)')
parser.add_argument('--modified_to', help='ISO datetime (UTC)')
parser.add_argument('--list', help='file with list of IDs')
parser.add_argument('--id', help='a single record ID')
parser.add_argument('--output_file', help='write XML as batch to this file')
parser.add_argument('--api_key', help='UNDL-issued api key')
parser.add_argument('--email', help='disabled')
parser.add_argument('--log', help='MDB connection string to write data to')

###

API_URL = 'https://digitallibrary.un.org/api/v1/record/'
LOG_DB_NAME = 'DLX_DL_log'
LOG_COLLECTION_NAME = 'log'
LOG_DATA = []

###

def main():
    args = parser.parse_args()
    DB.connect(args.connect)
    
    ## process arguments
    
    if args.api_key and args.log:
        cstr = args.log
        
        if cstr[0:9] == 'mongomock':
            cstr = 'mongodb://.../?authSource=' + LOG_DB_NAME
            log = MockClient(cstr)[LOG_DB_NAME][LOG_COLLECTION_NAME]
        else:
            log = MongoClient(cstr)[LOG_DB_NAME][LOG_COLLECTION_NAME]
    else:
        log = None
    
    cls = BibSet if args.type == 'bib' else AuthSet

    if args.id:
        rset = cls.from_query({'_id': int(args.id)})
    elif args.modified_from and args.modified_to:
        rset = cls.from_query({'updated': {'$gte': datetime.fromisoformat(args.modified_from), '$lt': datetime.fromisoformat(args.modified_to)}})
    elif args.modified_from:
        rset = cls.from_query({'updated': {'$gte': datetime.fromisoformat(args.modified_from)}})
    elif args.list:
        with open(args.list, 'r') as f:
            ids = [int(line) for line in f.readlines()]
            warn('Very long lists of IDs can use all the memory on the database server')
            rset = cls.from_query({'_id': {'$in': ids}})
    else:
        raise Exception('One of the arguments --id --modified_from --list is required')
    
    if args.output_file:
        if args.output_file.lower() == 'stdout':
            out = sys.stdout
        else:
            out = open(args.output_file, 'w')
    else:
        out = open(os.devnull, 'w')

    ## write
        
    out.write('<collection>')
    
    if args.type == 'bib':
        process_bibs(rset, out, args.api_key, args.email, log)
    else:
        process_auths(rset, out, args.api_key, args.email, log)
    
    out.write('</collection>')
    
    return LOG_DATA
    
###

def process_bibs(rset, out, api_key, email, log):
    for bib in rset:
        bib.delete_field('001')
        bib.delete_field('005')
        bib = _035(bib)
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
  
def post(rtype, rid, xml, api_key, email, log):
    headers = {
        'Authorization': 'Token ' + api_key,
        'Content-Type': 'application/xml'
    }
    
    params = {
        'mode': 'insertorreplace',
        #'callback_email': email
    }
    
    response = requests.post(API_URL, params=params, headers=headers, data=xml)
     
    logdata = {
        'time': datetime.now(timezone.utc), #.strftime('%Y-%m-%d %H:%M:%S'), 
        'record_type': rtype, 
        'record_id': rid, 
        'response_code': str(response.status_code), 
        'response_text': response.text.replace('\n', ''),
        'xml': xml
    }
    
    if log:
        log.insert_one(logdata)
    
    # clean for JSON serialization
    del logdata['_id'] # pymongo adds the _id key to the dict on insert??
    logdata['time'] = logdata['time'].strftime('%Y-%m-%d %H:%M:%S')
    
    LOG_DATA.append(logdata)
    
    print(json.dumps(logdata))

###

if __name__ == '__main__':
    main()

