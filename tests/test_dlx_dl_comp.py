import os, re, pytest, responses
from moto import mock_s3
from datetime import datetime
import dlx_dl_comp

os.environ['DLX_DL_TESTING'] = "true"

@pytest.fixture
@mock_s3 # this has to go after the fixture decorator
def db():
    from dlx import DB
    from dlx.marc import Bib, Auth
    
    DB.connect('mongomock://localhost') # mock connection always creates a fresh db?
    
    DB.bibs.drop()
    DB.auths.drop()
    
    Bib({'_id': 1}).set('998', 'c', '20200101000000').commit()
    Bib({'_id': 2}).set('998', 'c', '20200201000000').commit()
    Auth({'_id': 1}).set('998', 'c', '20200101000000').commit()
    
    DB.bibs.update_many({}, {'$set': {'updated': datetime.strptime('20200601', '%Y%m%d')}})
    DB.auths.update_many({}, {'$set': {'updated': datetime.strptime('20200601', '%Y%m%d')}})
    
    print(Bib.from_id(2).updated)
    
    return DB.client

@pytest.fixture
def excel_export_bib():
    from tempfile import NamedTemporaryFile
    
    f = open('btemp', 'wb')
    f.write(b'<html><table><tr><td>1</td><td>(DHL)1</td><td>20200101000000</td></tr><tr><td>2</td><td>(DHL)2</td><td>20200101000000</td><td>20200101000000</td></tr></table></html>')
    
    return f.name
    
@pytest.fixture
def excel_export_auth():
    from tempfile import NamedTemporaryFile
    
    f = open('atemp', 'wb')
    f.write(b'<html><table><tr><td>1</td><td>(DHLAUTH)1</td><td>20200101000000</td></tr><tr><td>2</td><td>(DHLAUTH)2</td><td>20200101000000</td><td>20200101000000</td></tr></table></html>')
    
    return f.name
    
def test_run(db, excel_export_bib, excel_export_auth, capsys):
    import dlx_dl_comp
    
    dlx_dl_comp.run(connect=db, file=excel_export_bib, changed='.', type='bib')
    assert capsys.readouterr().out == '2\t2020-01-01 00:00:00\t2020-02-01 00:00:00\n'
    
    dlx_dl_comp.run(connect=db, file=excel_export_auth, changed='.', type='auth')
    assert capsys.readouterr().out == ''
    
@responses.activate
def test_delete(db, excel_export_auth, capsys):
    from http.server import HTTPServer 
    from tempfile import NamedTemporaryFile
    from xmldiff.main import diff_texts
            
    server = HTTPServer(('127.0.0.1', 9090), None)
    responses.add(responses.POST, 'http://127.0.0.1:9090', body='test OK')
    responses.add(responses.GET, 'http://127.0.0.1:9090', body='test GET')
    dlx_dl_comp.API_URL = 'http://127.0.0.1:9090'
    dlx_dl_comp.run(connect=db, file=excel_export_auth, changed='.', type='auth', delete=True, api_key='x', nonce_key='x', callback_url='x')
    o = capsys.readouterr().out
    assert re.search('Candidates for deletion from DL: \[2\]', o)