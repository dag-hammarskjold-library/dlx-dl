import os, pytest, responses
from moto import mock_s3
from datetime import datetime
import dlx_dl

os.environ['DLX_DL_TESTING'] = "true"

START = datetime.now()

@pytest.fixture
@mock_s3 # this has to go after the fixture decorator
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
    
    S3.connect('mock_key', 'mock_key_id', 'mock_bucket')
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
    dlx_dl.run(connect=db, source='test', type='bib', id='1', output_file='STDOUT')
    assert diff_texts(capsys.readouterr().out, control) == []
    
    control = '<collection><record><datafield tag="035" ind1=" " ind2=" "><subfield code="a">(DHLAUTH)1</subfield></datafield><datafield tag="100" ind1=" " ind2=" "><subfield code="a">name_1</subfield></datafield><datafield tag="980" ind1=" " ind2=" "><subfield code="a">AUTHORITY</subfield></datafield><datafield tag="980" ind1=" " ind2=" "><subfield code="a">PERSONAL</subfield></datafield></record></collection>'
    dlx_dl.run(connect=db, source='test', type='auth', id='1', output_file='STDOUT')
    assert diff_texts(capsys.readouterr().out, control) == []
    
    # --ids
    dlx_dl.run(connect=db, source='test', type='auth', ids=['1'], output_file='STDOUT')
    assert diff_texts(capsys.readouterr().out, control) == []

def test_to_file(db, tmp_path):
    from xmldiff.main import diff_texts
    
    control = '<collection><record><datafield tag="035" ind1=" " ind2=" "><subfield code="a">(DHL)1</subfield></datafield><datafield tag="191" ind1=" " ind2=" "><subfield code="a">TEST/1</subfield></datafield><datafield tag="245" ind1=" " ind2=" "><subfield code="a">title_1</subfield></datafield><datafield tag="700" ind1=" " ind2=" "><subfield code="a">name_1</subfield><subfield code="0">(DHLAUTH)1</subfield></datafield><datafield tag="980" ind1=" " ind2=" "><subfield code="a">BIB</subfield></datafield><datafield tag="FFT" ind1=" " ind2=" "><subfield code="a">https://mock_bucket.s3.amazonaws.com/1e50210a0202497fb79bc38b6ade6c34</subfield><subfield code="d">English</subfield><subfield code="n">TEST_1-EN.pdf</subfield></datafield></record></collection>'
    out = tmp_path / 'out.xml'
    dlx_dl.run(connect=db, source='test', type='bib', id='1', output_file=out)
    assert diff_texts(out.read_text(), control) == []
    
def test_by_list(db, tmp_path, capsys):
    from xmldiff.main import diff_texts
    
    ids = tmp_path / 'ids.txt'
    ids.write_text('\n'.join([str(x) for x in (1, 2)]))
    
    control = '<collection><record><datafield tag="035" ind1=" " ind2=" "><subfield code="a">(DHL)1</subfield></datafield><datafield tag="191" ind1=" " ind2=" "><subfield code="a">TEST/1</subfield></datafield><datafield tag="245" ind1=" " ind2=" "><subfield code="a">title_1</subfield></datafield><datafield tag="700" ind1=" " ind2=" "><subfield code="a">name_1</subfield><subfield code="0">(DHLAUTH)1</subfield></datafield><datafield tag="980" ind1=" " ind2=" "><subfield code="a">BIB</subfield></datafield><datafield tag="FFT" ind1=" " ind2=" "><subfield code="a">https://mock_bucket.s3.amazonaws.com/1e50210a0202497fb79bc38b6ade6c34</subfield><subfield code="d">English</subfield><subfield code="n">TEST_1-EN.pdf</subfield></datafield></record><record><datafield tag="035" ind1=" " ind2=" "><subfield code="a">(DHL)2</subfield></datafield><datafield tag="245" ind1=" " ind2=" "><subfield code="a">title_2</subfield></datafield><datafield tag="700" ind1=" " ind2=" "><subfield code="a">name_2</subfield><subfield code="0">(DHLAUTH)2</subfield></datafield><datafield tag="980" ind1=" " ind2=" "><subfield code="a">BIB</subfield></datafield></record></collection>'
    dlx_dl.run(connect=db, source='test', type='bib', list=ids, output_file='STDOUT')
    assert diff_texts(capsys.readouterr().out, control) == []
    
def test_by_date(db, capsys):
    from xmldiff.main import diff_texts
    
    control = '<collection><record><datafield tag="035" ind1=" " ind2=" "><subfield code="a">(DHL)1</subfield></datafield><datafield tag="191" ind1=" " ind2=" "><subfield code="a">TEST/1</subfield></datafield><datafield tag="245" ind1=" " ind2=" "><subfield code="a">title_1</subfield></datafield><datafield tag="700" ind1=" " ind2=" "><subfield code="a">name_1</subfield><subfield code="0">(DHLAUTH)1</subfield></datafield><datafield tag="980" ind1=" " ind2=" "><subfield code="a">BIB</subfield></datafield><datafield tag="FFT" ind1=" " ind2=" "><subfield code="a">https://mock_bucket.s3.amazonaws.com/1e50210a0202497fb79bc38b6ade6c34</subfield><subfield code="d">English</subfield><subfield code="n">TEST_1-EN.pdf</subfield></datafield></record><record><datafield tag="035" ind1=" " ind2=" "><subfield code="a">(DHL)2</subfield></datafield><datafield tag="245" ind1=" " ind2=" "><subfield code="a">title_2</subfield></datafield><datafield tag="700" ind1=" " ind2=" "><subfield code="a">name_2</subfield><subfield code="0">(DHLAUTH)2</subfield></datafield><datafield tag="980" ind1=" " ind2=" "><subfield code="a">BIB</subfield></datafield></record></collection>'
    dlx_dl.run(connect=db, source='test', type='bib', modified_from=START.strftime('%Y-%m-%d'), output_file='STDOUT')
    assert diff_texts(capsys.readouterr().out, control) == []
     
    dlx_dl.run(connect=db, source='test', type='bib', modified_from=datetime.max.strftime('%Y-%m-%d'), output_file='STDOUT')
    assert capsys.readouterr().out == '<collection></collection>'
    
    dlx_dl.run(connect=db, source='test', type='bib', modified_within=100, output_file='STDOUT')
    assert diff_texts(capsys.readouterr().out, control) == []
    
    dlx_dl.run(connect=db, source='test', type='bib', modified_within=-1, output_file='STDOUT')
    assert capsys.readouterr().out == '<collection></collection>'
    
@responses.activate
def test_post_and_log(db, excel_export):
    from http.server import HTTPServer 
    from xmldiff.main import diff_texts
            
    server = HTTPServer(('127.0.0.1', 9090), None)
    responses.add(responses.POST, 'http://127.0.0.1:9090', body='test OK')
    dlx_dl.API_URL = 'http://127.0.0.1:9090'
    
    dlx_dl.run(connect=db, source='test', type='bib', modified_from=START.strftime('%Y-%m-%d'), api_key='x')
    
    entry = db['dummy']['dlx_dl_log'].find_one({'record_id': 1})
    assert entry['record_id'] == 1
    assert entry['response_code'] == 200
    assert entry['response_text'] == 'test OK'
    assert isinstance(entry['export_start'], datetime)
    assert isinstance(entry['time'], datetime)
    
    control = '<record><datafield tag="035" ind1=" " ind2=" "><subfield code="a">(DHL)1</subfield></datafield><datafield tag="191" ind1=" " ind2=" "><subfield code="a">TEST/1</subfield></datafield><datafield tag="245" ind1=" " ind2=" "><subfield code="a">title_1</subfield></datafield><datafield tag="700" ind1=" " ind2=" "><subfield code="a">name_1</subfield><subfield code="0">(DHLAUTH)1</subfield></datafield><datafield tag="980" ind1=" " ind2=" "><subfield code="a">BIB</subfield></datafield><datafield tag="FFT" ind1=" " ind2=" "><subfield code="a">https://mock_bucket.s3.amazonaws.com/1e50210a0202497fb79bc38b6ade6c34</subfield><subfield code="d">English</subfield><subfield code="n">TEST_1-EN.pdf</subfield></datafield></record>'
    assert diff_texts(entry['xml'], control) == []
    
    entry = db['dummy']['dlx_dl_log'].find_one({'source': 'test'})
    assert isinstance(entry['export_start'], datetime)
    entry = db['dummy']['dlx_dl_log'].find_one({'source': 'test', 'export_end': {'$exists': 1}})
    assert entry['record_type'] == 'bib'
    assert isinstance(entry['export_end'], datetime)
    
@responses.activate
def test_modified_since_log(db, capsys):
    from http.server import HTTPServer 
    from xmldiff.main import diff_texts
    from dlx import DB
    from dlx.marc import Bib

    server = HTTPServer(('127.0.0.1', 9090), None)
    responses.add(responses.POST, 'http://127.0.0.1:9090', body='test OK')
    dlx_dl.API_URL = 'http://127.0.0.1:9090'
    
    dlx_dl.run(connect=db, source='test', type='bib', modified_from=START.strftime('%Y-%m-%d'), api_key='x')
    capsys.readouterr().out # clear stdout
    Bib().set('999', 'a', 'new').commit()
    dlx_dl.run(connect=db, source='test', type='bib', modified_since_log=True, api_key='x')
    entry = db['dummy']['dlx_dl_log'].find_one({'record_id': 3})
    control = '<record><datafield tag="035" ind1=" " ind2=" "><subfield code="a">(DHL)3</subfield></datafield><datafield tag="980" ind1=" " ind2=" "><subfield code="a">BIB</subfield></datafield><datafield tag="999" ind1=" " ind2=" "><subfield code="a">new</subfield></datafield></record>'
    assert diff_texts(entry['xml'], control) == []
    
@responses.activate
def test_blacklist(db, capsys):
    from http.server import HTTPServer 
    from xmldiff.main import diff_texts
    
    server = HTTPServer(('127.0.0.1', 9090), None)
    responses.add(responses.POST, 'http://127.0.0.1:9090', body='test OK')
    dlx_dl.API_URL = 'http://127.0.0.1:9090'
    
    db['dummy']['blacklist'].insert_one({'symbol': 'TEST/1'})
    # control here has no FFT fields
    control = '<record><datafield tag="035" ind1=" " ind2=" "><subfield code="a">(DHL)1</subfield></datafield><datafield tag="191" ind1=" " ind2=" "><subfield code="a">TEST/1</subfield></datafield><datafield tag="245" ind1=" " ind2=" "><subfield code="a">title_1</subfield></datafield><datafield tag="700" ind1=" " ind2=" "><subfield code="a">name_1</subfield><subfield code="0">(DHLAUTH)1</subfield></datafield><datafield tag="980" ind1=" " ind2=" "><subfield code="a">BIB</subfield></datafield></record>'
    dlx_dl.run(connect=db, source='test', type='bib', modified_from=START.strftime('%Y-%m-%d'), api_key='x')
    entry = db['dummy']['dlx_dl_log'].find_one({'record_id': 1})
    assert diff_texts(entry['xml'], control) == []

@responses.activate   
def test_queue(db, capsys):
    import time, json
    from http.server import HTTPServer 
    from xmldiff.main import diff_texts
    
    server = HTTPServer(('127.0.0.1', 9090), None)
    responses.add(responses.POST, 'http://127.0.0.1:9090', body='test OK')
    dlx_dl.API_URL = 'http://127.0.0.1:9090'

    dlx_dl.run(connect=db, source='test', type='bib', modified_from=START.strftime('%Y-%m-%d'), api_key='x', queue=1)
    data = list(filter(None, capsys.readouterr().out.split('\n')))
    assert len(data) == 1
    assert json.loads(data[0])['record_id'] == 1
    
    time.sleep(.1)
    dlx_dl.run(connect=db, source='test', type='bib', api_key='x', modified_within=0, queue=1)
    data = list(filter(None, capsys.readouterr().out.split('\n')))
    assert len(data) == 1
    assert json.loads(data[0])['record_id'] == 2
    
    # queued record is deleted
    time.sleep(.1)
    db['dummy']['dlx_dl_queue'].insert_one({'record_id': 42, 'source': 'test', 'type': 'bib'})
    dlx_dl.run(connect=db, source='test', type='bib', api_key='x', modified_within=0, queue=1)
    data = list(filter(None, capsys.readouterr().out.split('\n')))
    assert len(data) == 0
    assert db['dummy']['dlx_dl_queue'].find_one({}) == None
    
### end