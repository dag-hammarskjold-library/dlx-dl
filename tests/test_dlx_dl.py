import os, pytest, responses
from moto import mock_aws
from datetime import datetime
from dlx_dl.scripts import export, sync

os.environ['DLX_DL_TESTING'] = "true"
START = datetime.now()
export.API_URL = 'http://127.0.0.1:9090/record'
sync.API_RECORD_URL = 'http://127.0.0.1:9090/record'
sync.API_SEARCH_URL = 'http://127.0.0.1:9090/search'

@pytest.fixture
@mock_aws # this has to go after the fixture decorator
def db():
    from dlx import DB
    from dlx.marc import Bib, Auth
    from dlx.file import S3, File, Identifier
    from tempfile import TemporaryFile

    DB.connect('mongomock://localhost') # ? does mock connection create a fresh db ?
    
    DB.bibs.drop()
    DB.auths.drop()
    DB.files.drop()
    DB.handle['dlx_dl_log'].drop()
    
    Auth().set('100', 'a', 'name_1').commit()
    Auth().set('100', 'a', 'name_2').commit()
    
    Bib().set('191', 'a', 'TEST/1').set('245', 'a', 'title_1').set('700', 'a', 1).commit()
    Bib().set('245', 'a', 'title_2').set('700', 'a', 2).commit()
    
    S3.connect(access_key='key', access_key_id='key_id', bucket='mock_bucket')
    S3.client.create_bucket(Bucket=S3.bucket)
    
    handle = TemporaryFile()
    handle.write(b'some data')
    handle.seek(0)
    File.import_from_handle(
        handle,
        filename='',
        identifiers=[Identifier('symbol', 'TEST/1')],
        languages=['EN'], 
        mimetype='text/plain', 
        source='test'
    )

    return DB.client

@pytest.fixture
def mock_post(scope='module', autoyield=True):
    with responses.RequestsMock() as rsps:
        rsps.add(responses.POST, 'http://127.0.0.1:9090/record', body='test OK')
    
        yield rsps 

@pytest.fixture
def mock_get_post(scope='module', autoyield=True):
    with responses.RequestsMock() as rsps:
        rsps.add(responses.GET, 'http://127.0.0.1:9090/search', body='<record></record>', status=200)
        rsps.add(responses.POST, 'http://127.0.0.1:9090/record', body='test OK')
    
        yield rsps
    
@pytest.fixture
def excel_export():
    from tempfile import NamedTemporaryFile
    
    f = NamedTemporaryFile('r+')
    f.write('<html><table><tr><td>1</td><td>(DHL)1</td><td>20200101000000</td></tr><tr><td>2</td><td>(DHL)2</td><td>20200101000000</td></tr></table></html>')
    f.seek(0)
    
    return f

def test_by_id(db, capsys): # capsys is a Pytest builtin fixture
    from dlx.marc import Auth, Bib, BibSet
    from xmldiff.main import diff_texts
    
    control = '<collection><record><datafield tag="035" ind1=" " ind2=" "><subfield code="a">(DHL)1</subfield></datafield><datafield tag="191" ind1=" " ind2=" "><subfield code="a">TEST/1</subfield></datafield><datafield tag="245" ind1=" " ind2=" "><subfield code="a">title_1</subfield></datafield><datafield tag="700" ind1=" " ind2=" "><subfield code="a">name_1</subfield><subfield code="0">(DHLAUTH)1</subfield></datafield><datafield tag="980" ind1=" " ind2=" "><subfield code="a">BIB</subfield></datafield><datafield tag="FFT" ind1=" " ind2=" "><subfield code="a">https://mock_bucket.s3.amazonaws.com/1e50210a0202497fb79bc38b6ade6c34</subfield><subfield code="d">English</subfield><subfield code="n">TEST_1-EN.pdf</subfield></datafield></record></collection>'
    export.run(connect=db, source='test', type='bib', id='1', xml='STDOUT')
    assert diff_texts(capsys.readouterr().out, control) == []
    
    control = '<collection><record><datafield tag="035" ind1=" " ind2=" "><subfield code="a">(DHLAUTH)1</subfield></datafield><datafield tag="100" ind1=" " ind2=" "><subfield code="a">name_1</subfield></datafield><datafield tag="980" ind1=" " ind2=" "><subfield code="a">AUTHORITY</subfield></datafield><datafield tag="980" ind1=" " ind2=" "><subfield code="a">PERSONAL</subfield></datafield></record></collection>'
    export.run(connect=db, source='test', type='auth', id='1', xml='STDOUT')
    assert diff_texts(capsys.readouterr().out, control) == []
    
    # --ids
    export.run(connect=db, source='test', type='auth', ids=['1'], xml='STDOUT')
    assert diff_texts(capsys.readouterr().out, control) == []

def test_to_file(db, tmp_path):
    from xmldiff.main import diff_texts
    
    control = '<collection><record><datafield tag="035" ind1=" " ind2=" "><subfield code="a">(DHL)1</subfield></datafield><datafield tag="191" ind1=" " ind2=" "><subfield code="a">TEST/1</subfield></datafield><datafield tag="245" ind1=" " ind2=" "><subfield code="a">title_1</subfield></datafield><datafield tag="700" ind1=" " ind2=" "><subfield code="a">name_1</subfield><subfield code="0">(DHLAUTH)1</subfield></datafield><datafield tag="980" ind1=" " ind2=" "><subfield code="a">BIB</subfield></datafield><datafield tag="FFT" ind1=" " ind2=" "><subfield code="a">https://mock_bucket.s3.amazonaws.com/1e50210a0202497fb79bc38b6ade6c34</subfield><subfield code="d">English</subfield><subfield code="n">TEST_1-EN.pdf</subfield></datafield></record></collection>'
    out = tmp_path / 'out.xml'
    export.run(connect=db, source='test', type='bib', id='1', xml=out)
    assert diff_texts(out.read_text(), control) == []
    
def test_by_list(db, tmp_path, capsys):
    from xmldiff.main import diff_texts
    
    ids = tmp_path / 'ids.txt'
    ids.write_text('\n'.join([str(x) for x in (1, 2)]))
    
    control = '<collection><record><datafield tag="035" ind1=" " ind2=" "><subfield code="a">(DHL)1</subfield></datafield><datafield tag="191" ind1=" " ind2=" "><subfield code="a">TEST/1</subfield></datafield><datafield tag="245" ind1=" " ind2=" "><subfield code="a">title_1</subfield></datafield><datafield tag="700" ind1=" " ind2=" "><subfield code="a">name_1</subfield><subfield code="0">(DHLAUTH)1</subfield></datafield><datafield tag="980" ind1=" " ind2=" "><subfield code="a">BIB</subfield></datafield><datafield tag="FFT" ind1=" " ind2=" "><subfield code="a">https://mock_bucket.s3.amazonaws.com/1e50210a0202497fb79bc38b6ade6c34</subfield><subfield code="d">English</subfield><subfield code="n">TEST_1-EN.pdf</subfield></datafield></record><record><datafield tag="035" ind1=" " ind2=" "><subfield code="a">(DHL)2</subfield></datafield><datafield tag="245" ind1=" " ind2=" "><subfield code="a">title_2</subfield></datafield><datafield tag="700" ind1=" " ind2=" "><subfield code="a">name_2</subfield><subfield code="0">(DHLAUTH)2</subfield></datafield><datafield tag="980" ind1=" " ind2=" "><subfield code="a">BIB</subfield></datafield></record></collection>'
    export.run(connect=db, source='test', type='bib', list=ids, xml='STDOUT')
    assert diff_texts(capsys.readouterr().out, control) == []
    
def test_by_date(db, capsys):
    from xmldiff.main import diff_texts
    
    control = '<collection><record><datafield tag="035" ind1=" " ind2=" "><subfield code="a">(DHL)1</subfield></datafield><datafield tag="191" ind1=" " ind2=" "><subfield code="a">TEST/1</subfield></datafield><datafield tag="245" ind1=" " ind2=" "><subfield code="a">title_1</subfield></datafield><datafield tag="700" ind1=" " ind2=" "><subfield code="a">name_1</subfield><subfield code="0">(DHLAUTH)1</subfield></datafield><datafield tag="980" ind1=" " ind2=" "><subfield code="a">BIB</subfield></datafield><datafield tag="FFT" ind1=" " ind2=" "><subfield code="a">https://mock_bucket.s3.amazonaws.com/1e50210a0202497fb79bc38b6ade6c34</subfield><subfield code="d">English</subfield><subfield code="n">TEST_1-EN.pdf</subfield></datafield></record><record><datafield tag="035" ind1=" " ind2=" "><subfield code="a">(DHL)2</subfield></datafield><datafield tag="245" ind1=" " ind2=" "><subfield code="a">title_2</subfield></datafield><datafield tag="700" ind1=" " ind2=" "><subfield code="a">name_2</subfield><subfield code="0">(DHLAUTH)2</subfield></datafield><datafield tag="980" ind1=" " ind2=" "><subfield code="a">BIB</subfield></datafield></record></collection>'
    #export.run(connect=db, source='test', type='bib', modified_from=START.strftime('%Y-%m-%d'), xml='STDOUT')
    #assert diff_texts(capsys.readouterr().out, control) == []
     
    #export.run(connect=db, source='test', type='bib', modified_from=datetime.max.strftime('%Y-%m-%d'), xml='STDOUT')
    #assert capsys.readouterr().out == '<collection></collection>'
    
    export.run(connect=db, source='test', type='bib', modified_within=100, xml='STDOUT')
    #assert diff_texts(capsys.readouterr().out, control) == []
    
    export.run(connect=db, source='test', type='bib', modified_within=-1, xml='STDOUT')
    #assert capsys.readouterr().out == '<collection></collection>'
    
def test_post_and_log(db, capsys, excel_export, mock_post):
    from http.server import HTTPServer 
    from xmldiff.main import diff_texts
    from dlx import DB

    export.run(connect=db, source='test', type='bib', modified_within=100, use_api=True, api_key='x')

    entry = DB.handle['dlx_dl_log'].find_one({})
    assert entry['record_id'] == 1
    assert entry['response_code'] == 200
    assert entry['response_text'] == 'test OK'
    assert isinstance(entry['export_start'], datetime)
    assert isinstance(entry['time'], datetime)
    
    control = '<record><datafield tag="035" ind1=" " ind2=" "><subfield code="a">(DHL)1</subfield></datafield><datafield tag="191" ind1=" " ind2=" "><subfield code="a">TEST/1</subfield></datafield><datafield tag="245" ind1=" " ind2=" "><subfield code="a">title_1</subfield></datafield><datafield tag="700" ind1=" " ind2=" "><subfield code="a">name_1</subfield><subfield code="0">(DHLAUTH)1</subfield></datafield><datafield tag="980" ind1=" " ind2=" "><subfield code="a">BIB</subfield></datafield><datafield tag="FFT" ind1=" " ind2=" "><subfield code="a">https://mock_bucket.s3.amazonaws.com/1e50210a0202497fb79bc38b6ade6c34</subfield><subfield code="d">English</subfield><subfield code="n">TEST_1-EN.pdf</subfield></datafield></record>'
    assert diff_texts(entry['xml'], control) == []
    
    entry = DB.handle['dlx_dl_log'].find_one({'source': 'test'})
    assert isinstance(entry['export_start'], datetime)
    entry = DB.handle['dlx_dl_log'].find_one({'source': 'test', 'export_end': {'$exists': 1}})
    assert entry['record_type'] == 'bib'
    assert isinstance(entry['export_end'], datetime)
    
def test_modified_since_log(db, capsys, mock_post):
    from http.server import HTTPServer 
    from xmldiff.main import diff_texts
    from dlx import DB
    from dlx.marc import Bib

    export.run(connect=db, source='test', type='bib', modified_within=100, use_api=True, api_key='x')
    capsys.readouterr().out # clear stdout
    Bib().set('999', 'a', 'new').commit()
    export.run(connect=db, source='test', type='bib', modified_since_log=True, use_api=True, api_key='x')
    entry = DB.handle['dlx_dl_log'].find_one({'record_id': 3})
    control = '<record><datafield tag="035" ind1=" " ind2=" "><subfield code="a">(DHL)3</subfield></datafield><datafield tag="980" ind1=" " ind2=" "><subfield code="a">BIB</subfield></datafield><datafield tag="999" ind1=" " ind2=" "><subfield code="a">new</subfield></datafield></record>'
    assert diff_texts(entry['xml'], control) == []
    
def test_blacklist(db, capsys, mock_post):
    from dlx import DB
    from http.server import HTTPServer 
    from xmldiff.main import diff_texts

    DB.handle['blacklist'].insert_one({'symbol': 'TEST/1'})
    # control here has no FFT fields
    control = '<record><datafield tag="035" ind1=" " ind2=" "><subfield code="a">(DHL)1</subfield></datafield><datafield tag="191" ind1=" " ind2=" "><subfield code="a">TEST/1</subfield></datafield><datafield tag="245" ind1=" " ind2=" "><subfield code="a">title_1</subfield></datafield><datafield tag="700" ind1=" " ind2=" "><subfield code="a">name_1</subfield><subfield code="0">(DHLAUTH)1</subfield></datafield><datafield tag="980" ind1=" " ind2=" "><subfield code="a">BIB</subfield></datafield></record>'
    export.run(connect=db, source='test', type='bib', modified_within=100, use_api=True, api_key='x')
    entry = DB.handle['dlx_dl_log'].find_one({'record_id': 1})
    assert diff_texts(entry['xml'], control) == []
  
def test_queue(db, capsys, mock_post):
    import time, json
    from http.server import HTTPServer 
    from xmldiff.main import diff_texts

    export.run(connect=db, source='test', type='bib', modified_within=100, use_api=True, api_key='x', queue=1)
    data = list(filter(None, capsys.readouterr().out.split('\n')))
    #assert len(data) == 1
    #assert json.loads(data[0])['record_id'] == 1
    
    time.sleep(.1)
    export.run(connect=db, source='test', type='bib', use_api=True, api_key='x', modified_within=0, queue=1)
    data = list(filter(None, capsys.readouterr().out.split('\n')))
    #assert len(data) == 1
    #assert json.loads(data[0])['record_id'] == 2
    
    # queued record is deleted
    time.sleep(.1)
    db['dummy']['dlx_dl_queue'].insert_one({'record_id': 42, 'source': 'test', 'type': 'bib'})
    export.run(connect=db, source='test', type='bib', use_api=True, api_key='x', modified_within=0, queue=1)
    data = list(filter(None, capsys.readouterr().out.split('\n')))
    #assert len(data) == 0
    #assert db['dummy']['dlx_dl_queue'].find_one({}) == None
   
def test_delete(db, capsys, mock_post):
    import json
    from http.server import HTTPServer 
    from xmldiff.main import diff_texts
    from dlx.marc import Bib
    
    bib = Bib().set('245', 'a', 'Will self destruct')
    bib.commit()
    bib.delete()

    export.run(connect=db, source='test', type='bib', modified_within=100, use_api=True, api_key='x')
    data = list(filter(None, capsys.readouterr().out.split('\n')))
    #assert len(data) == 3
    #assert json.loads(data[2])['record_id'] == 3

def test_sync(db, capsys, mock_get_post):
    from http.server import HTTPServer
    from dlx.marc import Bib
    
    bib = Bib().set('245', 'a', 'Will self destruct')
    bib.commit()
    bib.delete()

    sync.run(connect=db, source='test', type='bib', modified_within=100, force=True)
    data = list(filter(None, capsys.readouterr().out.split('\n')))
    assert data
        
### end