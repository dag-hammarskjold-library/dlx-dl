import sys, os, re, json, requests
from datetime import datetime, timezone
from argparse import ArgumentParser
from dlx import DB, Config
from dlx.marc import Bib, BibSet, Auth, AuthSet
from pymongo import MongoClient
from mongomock import MongoClient as MockClient
from pandas import concat, read_html

###

parser = ArgumentParser()
parser.add_argument('--connect', required=True, help='dlx connection string')
parser.add_argument('--file', required=True, help='An "Excel export" with parameter "035__a,998__z"')
parser.add_argument('--modified_from', required=True)
parser.add_argument('--modified_to')
parser.add_argument('--type', required=True, choices=['bib', 'auth'])
parser.add_argument('--api_key', help='UNDL-issued api key')
parser.add_argument('--email', help='disabled')
parser.add_argument('--callback_url', help="A URL that can receive the results of a submitted task.")
parser.add_argument('--nonce_key', help='A validation key that will be passed to and from the UNDL API.')

API_URL = 'https://digitallibrary.un.org/api/v1/record/'

def run(**kwargs):
    if kwargs:
        sys.argv[1:] = ['--{}={}'.format(key, val) for key, val in kwargs.items()]
        
    args = parser.parse_args()
    
    if isinstance(kwargs.get('connect'), (MongoClient, MockClient)):
        DB.client = kwargs['connect']
    else:
        DB.connect(args.connect)

    dataframe = concat(read_html(open(args.file).read()))    
    dl_last = {}
    
    for index, row in dataframe.iterrows():
        id_pattern = r'\(DHL\)(\d+)' if args.type == 'bib' else r'\(DHLAUTH\)(\d+)'
        match = re.search(id_pattern, row[1])

        if match:
            rid = int(match.group(1))    
            dl_last[rid] = row[2]

    cls = BibSet if args.type == 'bib' else AuthSet
    
    modified_from = datetime.strptime(args.modified_from, '%Y-%m-%d')
    modified_to = datetime.strptime(args.modified_to, '%Y-%m-%d') if args.modified_to else datetime.now(timezone.utc)
            
    in_dlx = []
    
    for record in cls.from_query({'updated': {'$gte': modified_from, '$lt': modified_to}}, projection={'998': 1}):
        ldl = dl_last.get(record.id, 0)   
        ldlx = record.get_value('998', 'z') or 0
        
        if int(ldl) < int(ldlx):
            print('\t'.join([str(record.id), str(ldl), str(ldlx)]))
        
        in_dlx.append(record.id)
        
    # delete
    if args.api_key:
        assert args.nonce_key
        assert args.callback_url
    
        dset = cls()
                
        for idx in dl_last.keys():
            if idx not in in_dlx:
                (xrecord, pre) = (Bib(), '(DHL)') if args.type == 'bib' else (Auth(), '(DHLAUTH)')
                
                xrecord.id = idx
                xrecord.set('035', 'a', f'{pre}{idx}')
                xrecord.set('980', 'a', 'DELETED')
                
                dset.records.append(xrecord)
                
        print(f'Candidates for deletion from DL: {[r.id for r in dset.records]}')
        
        if 'DLX_DL_TESTING' not in os.environ:
            p = input('Proceed? (Y/N) ')
            
            if p.upper() != 'Y':
                print('quitting')
                exit()
                    
        headers = {
            'Authorization': 'Token ' + args.api_key,
            'Content-Type': 'application/xml; charset=utf-8',
        }
        
        nonce = {
            'type': args.type,
            'id': rid,
            'key': args.nonce_key
        }
        
        params = {
            'mode': 'insertorreplace',
            'callback_email': args.email,
            'callback_url': args.callback_url,
            'nonce': json.dumps(nonce)
        }
        
        for xrecord in dset.records:
            response = requests.post(API_URL, params=params, headers=headers, data=xrecord.to_xml().encode('utf-8'))
            print(f'{response.status_code} : {response.text}')

###   
    
if __name__ == '__main__':            
    run()
