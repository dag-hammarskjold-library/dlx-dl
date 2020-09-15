import sys, re, csv
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
            
    for record in cls.from_query({'updated': {'$gte': modified_from, '$lt': modified_to}}, projection={'998': 1}):
        ldl = dl_last.get(record.id, 0)    
        ldlx = record.get_value('998', 'z')
        
        if int(ldl) < int(ldlx):
            print('\t'.join([str(record.id), str(ldl), str(ldlx)]))

###   
    
if __name__ == '__main__':            
    run()
