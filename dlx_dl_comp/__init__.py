import sys, os, re, json, requests
from datetime import datetime, time, timezone, timedelta
from argparse import ArgumentParser
from dlx import DB, Config
from dlx.marc import Bib, BibSet, Auth, AuthSet
from pymongo import MongoClient
from bson import Regex
from mongomock import MongoClient as MockClient
from pandas import concat, read_html

###

parser = ArgumentParser()
parser.add_argument('--connect', required=True, help='DLX connection string')
parser.add_argument('--file', required=True, help='An "Excel export" with parameter "035__a,998__a,998__c"')
parser.add_argument('--created', required=True, help='String to match with 998$a')
parser.add_argument('--type', required=True, choices=['bib', 'auth'])
parser.add_argument('--delete', action='store_true', help='Boolean')
parser.add_argument('--api_key', help='UNDL-issued api key')
parser.add_argument('--email', help='disabled')
parser.add_argument('--callback_url', help="A URL that can receive the results of a submitted task")
parser.add_argument('--nonce_key', help='A validation key that will be passed to and from the UNDL API')

API_URL = 'https://digitallibrary.un.org/api/v1/record/'

def run(**kwargs):
    if kwargs:
        sys.argv[1:] = ['--{}={}'.format(key, val) for key, val in kwargs.items()]
       
    args = parser.parse_args()
    out = open(f'{args.created}.txt', 'w')
    
    if isinstance(kwargs.get('connect'), (MongoClient, MockClient)):
        DB.client = kwargs['connect']
    else:
        DB.connect(args.connect)
    
    # index dl
    dataframe = concat(read_html(open(args.file, 'rb').read()))    
    dl_last = {}

    for index, row in dataframe.iterrows():
        id_pattern = r'\(DHL\)(\d+)' if args.type == 'bib' else r'\(DHLAUTH\)(\d+)'
        match = re.search(id_pattern, row[1])

        if match:
            rid = int(match.group(1))
            dl_last[rid] = row[2] if str(row[3]).lower() == 'nan' else row[3]

    # index dlx
    cls = BibSet if args.type == 'bib' else AuthSet
    cr = args.created.replace('-', '')

    q = {
        '998.subfields': {
            '$elemMatch': {
                'code': 'a', 
                'value': Regex(f'^{cr}')
            }
        },
        'updated': {'$lt': datetime.now().replace(hour=0, minute=0, second=0)}
    }
    
    # compare
    for record in cls.from_query(q, projection={'998': 1}):
        ldl = dl_last.get(record.id)
        
        try:
            int(ldl)
        except:
            ldl = None
        
        if ldl is None:
            ldl = datetime.min
        else:
            ldl = datetime.strptime(ldl, '%Y%m%d%H%M%S')

        ldlx = record.get_value('998', 'c') or record.get_value('998', 'a')
        ldlx = datetime.strptime(ldlx, '%Y%m%d%H%M%S')
        
        if ldlx - ldl in (timedelta(hours=4), timedelta(hours=5)):
            # NY / UTC timezone ambiguity
            pass
        elif ldl < ldlx:
            text = '\t'.join([str(record.id), str(ldl), str(ldlx)])
            print(text)
            out.write(text + '\n')
            
    out.close()
        
    # delete
    if args.delete:
        assert args.api_key
        assert args.nonce_key
        assert args.callback_url
        
        ids = list(dl_last.keys())
        inc = 50000
        chunks = int(len(ids) / inc) + 1
        start = 0
        end = inc
        seen = []
        
        for chunk in range(0, chunks):
            # prevents query string from being too long
            rset = cls.from_query({'_id': {'$in': ids[start:end]}}, projection={'_id': 1})
            seen += [r.id for r in rset]
            start += inc
            end += inc
        
        to_delete = []
        
        for idx in filter(lambda x: x not in seen, ids):
            (xrecord, pre) = (Bib(), '(DHL)') if args.type == 'bib' else (Auth(), '(DHLAUTH)')
            xrecord.id = idx
            xrecord.set('035', 'a', f'{pre}{idx}')
            xrecord.set('980', 'a', 'DELETED')
            to_delete.append(xrecord)

        print(f'Candidates for deletion from DL: {[r.id for r in to_delete]}')
        
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
        
        for xrecord in to_delete:
            response = requests.post(API_URL, params=params, headers=headers, data=xrecord.to_xml().encode('utf-8'))
            print(f'{response.status_code} : {response.text}')

###   
    
if __name__ == '__main__':            
    run()
