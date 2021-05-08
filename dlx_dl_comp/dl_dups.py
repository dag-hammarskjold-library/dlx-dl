import sys, os, re, json
from argparse import ArgumentParser
from pandas import concat, read_html
from dlx.marc import Bib

ap = ArgumentParser()
ap.add_argument('--files', required=True, nargs='+', help='Variable-length list of file paths. Files are "Excel exports" with paramerer "035__a,998__a,998__c"')
ap.add_argument('--type', required=True, choices=['auth', 'bib'])
ap.add_argument('--out', help='Path to write deletion XML')

def run():
    args = ap.parse_args()
    
    if args.out:
        out = open(args.out, 'w')
    else:
        out = sys.stdout
        
    dfs = []
    
    for f in args.files:
        dfs += read_html(open(f, 'rb').read())
    
    dataframe = concat(dfs)
    seen = {}

    for index, row in dataframe.iterrows():
        match = re.search('(\(DHL|DHLAUTH\))(\d+)', row[1])

        if match:
            rid = int(match.group(2))
            dl_last = row[2] if str(row[3]).lower() == 'nan' else row[3]
            dl_ids = seen.get(rid)
            
            if dl_ids:
                dl_ids[row[0]] = dl_last
            else:
                seen[rid] = {row[0]: dl_last}
 
    print("dupes found:")
    to_delete = []
    
    for rid, dl_ids in seen.items():
        if len(dl_ids) > 1:
            print(f'{rid}\t' + '\t'.join(map(lambda x: str(x), dl_ids.keys())))
            to_delete += sorted(dl_ids.keys(), key=lambda x: dl_ids[x])[0:-1]
    
    if len(to_delete) == 0:
        print('none found :)')
        exit()
        
    for idx in to_delete:
        xrecord = Bib()
        xrecord.set('001', 'a', str(idx))
        xrecord.set('980', 'a', 'DELETED')
 
        out.write(xrecord.to_xml() + '\n')
    
    if args.out:
        print('wrote deletion XML to ' + args.out)
            
if __name__ == '__main__':
    run()
