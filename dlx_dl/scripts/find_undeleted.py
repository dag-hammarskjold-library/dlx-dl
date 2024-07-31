'''Writes a report of records that have been deleted in unbis but are still in undl'''

import sys, os, re, json, requests, time
from argparse import ArgumentParser
from xml.etree import ElementTree
from dlx import DB
from dlx.marc import Bib, Auth
from dlx_dl.scripts import sync
from boto3 import client as botoclient

API_SEARCH_URL = 'https://digitallibrary.un.org/api/v1/search'
NS = '{http://www.loc.gov/MARC21/slim}'
API_KEY = botoclient('ssm', region_name='us-east-1').get_parameter(Name='undl-dhl-metadata-api-key')['Parameter']['Value']

def get_args():
    ap = ArgumentParser()
    ap.add_argument('--connect', help='MDB connection string')
    ap.add_argument('--database', help='Database name')
    ap.add_argument('--type', choices=['bib', 'auth'])
    ap.add_argument('--query', help='UNDL query string')
    ap.add_argument('--output_file', help='Path to the file to write results to')

    return ap.parse_args()

def run():
    args = get_args()
    DB.connect(args.connect, database=args.database)
    output_file = args.output_file or f'{time.time()}.txt'
    OUT = open(output_file, 'w')
    HEADERS = {'Authorization': 'Token ' + API_KEY}
    search_id = ''
    url = f'{API_SEARCH_URL}?search_id={search_id}&p={args.query or ""}&format=xml'
    page = 1
    total = 0
    seen = 0
    status = f'page: {page}'

    while 1:
        if args.type == 'auth':
            url += '&c=Authorities'

        response = requests.get(url, headers=HEADERS)
       
        if response.status_code == 429:
            print('\nRate limit reached. Waiting five mintues to retry...')
            time.sleep(60 * 5)
            print('OK, resuming.')
            continue
        elif not response.ok:
            raise Exception(f'{response.status_code}: {response.text}')

        root = ElementTree.fromstring(response.text)
        col = root.find(f'{NS}collection')

        if len(col) == 0:
            break
        
        search_id = root.find('search_id').text
        total = int(root.find('total').text or 0)

        if page == 1:
            print(f'Found {total} records')
            print('Writing results to ' + output_file)

        dl_ids = []
    
        for rec in col:
            seen += 1
            dl_record = (Bib if args.type == 'bib' else Auth).from_xml_raw(rec)
            _035 = next(filter(lambda x: re.match('^\(DHL', x), dl_record.get_values('035', 'a')), '')
        
            if match := re.match('^\((DHL|DHLAUTH)\)(.*)', _035):
                dl_record.id = int(match.group(2))
                dl_ids.append(dl_record.id)

        dlx_ids = [x['_id'] for x in (DB.bibs if args.type == 'bib' else DB.auths).find({'_id': {'$in': dl_ids}}, projection={'_id': 1})]
        
        if not_in_dlx := [x for x in dl_ids if x not in dlx_ids]:
            OUT.write('\n'.join([str(x) for x in not_in_dlx]) + '\n')
            OUT.flush()

        url = f'{API_SEARCH_URL}?search_id={search_id}&p=&format=xml'
        print('\b' * len(status) + f'page: {page}', end='', flush=True)
        page += 1
        status = f'page: {page}'

    if total == 0:
        print('No records found')
    elif seen != total:
        print(response.text)
        raise Exception(f'Only {seen}/{total} of the DL records were seen. The API may not have returned all the results')

### 

if __name__ == '__main__':
    run()