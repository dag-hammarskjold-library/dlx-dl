import pytest, responses
from moto import mock_s3
from datetime import datetime
import dlx_dl

START = datetime.now()

@pytest.fixture
@mock_s3 # this has to go after the fixture decorator
def db():
    from dlx import DB
    from dlx.marc import Bib, Auth
    from dlx.file import S3, File, Identifier
    from tempfile import TemporaryFile
    
    DB.connect('mongomock://localhost') # mock connection always creates a fresh db 
    
    Bib().set('191', 'a', 'TEST/1').set('245', 'a', 'title_1').set('700', 'a', 1).commit()
    Bib().set('245', 'a', 'title_2').set('700', 'a', 2).commit()
    
    Auth().set('100', 'a', 'name_1').commit()
    Auth().set('100', 'a', 'name_2').commit()
    
    S3.connect('mock_key', 'mock_key_id', 'mock_bucket')
    S3.client.create_bucket(Bucket=S3.bucket)
    
    handle = TemporaryFile()
    handle.write(b'some data')
    handle.seek(0)
    File.import_from_handle(handle, 'filename', [Identifier('symbol', 'TEST/1')], ['EN'], '', 'test')

    return DB.client
    
def test_by_id(db, capsys): # capsys is a Pytest builtin fixture
    from dlx.marc import Auth, Bib, BibSet
    from xmldiff.main import diff_texts
    
    control = '<collection><record><datafield ind1=" " ind2=" " tag="035"><subfield code="a">(DHL)1</subfield></datafield><datafield ind1=" " ind2=" " tag="191"><subfield code="a">TEST/1</subfield></datafield><datafield ind1=" " ind2=" " tag="245"><subfield code="a">title_1</subfield></datafield><datafield ind1=" " ind2=" " tag="700"><subfield code="a">1</subfield></datafield><datafield ind1=" " ind2=" " tag="980"><subfield code="a">BIB</subfield></datafield><datafield ind1=" " ind2=" " tag="FFT"><subfield code="a">https://mock_bucket.s3.amazonaws.com/1e50210a0202497fb79bc38b6ade6c34</subfield><subfield code="d">English</subfield><subfield code="n">TEST_1-EN.pdf</subfield></datafield></record></collection>'
    
    dlx_dl.run(connect=db, type='bib', id='1', output_file='STDOUT')
    assert diff_texts(capsys.readouterr().out, control) == []
    
    control = '<collection><record><datafield ind1=" " ind2=" " tag="035"><subfield code="a">(DHLAUTH)1</subfield></datafield><datafield ind1=" " ind2=" " tag="100"><subfield code="a">name_1</subfield></datafield><datafield ind1=" " ind2=" " tag="980"><subfield code="a">AUTHORITY</subfield></datafield><datafield ind1=" " ind2=" " tag="980"><subfield code="a">PERSONAL</subfield></datafield></record></collection>'
    
    dlx_dl.run(connect=db, type='auth', id='1', output_file='STDOUT')
    assert diff_texts(capsys.readouterr().out, control) == []
    
def test_to_file(db, tmp_path):
    from xmldiff.main import diff_texts
    
    control = '<collection><record><datafield ind1=" " ind2=" " tag="035"><subfield code="a">(DHL)1</subfield></datafield><datafield ind1=" " ind2=" " tag="191"><subfield code="a">TEST/1</subfield></datafield><datafield ind1=" " ind2=" " tag="245"><subfield code="a">title_1</subfield></datafield><datafield ind1=" " ind2=" " tag="700"><subfield code="a">1</subfield></datafield><datafield ind1=" " ind2=" " tag="980"><subfield code="a">BIB</subfield></datafield><datafield ind1=" " ind2=" " tag="FFT"><subfield code="a">https://mock_bucket.s3.amazonaws.com/1e50210a0202497fb79bc38b6ade6c34</subfield><subfield code="d">English</subfield><subfield code="n">TEST_1-EN.pdf</subfield></datafield></record></collection>'

    out = tmp_path / 'out.xml'
    dlx_dl.run(connect=db, type='bib', id='1', output_file=out)
    assert diff_texts(out.read_text(), control) == []
    
def test_by_list(db, tmp_path, capsys):
    from xmldiff.main import diff_texts
    
    ids = tmp_path / 'ids.txt'
    ids.write_text('\n'.join([str(x) for x in (1, 2)]))
    
    control = '<collection><record><datafield ind1=" " ind2=" " tag="035"><subfield code="a">(DHL)1</subfield></datafield><datafield ind1=" " ind2=" " tag="191"><subfield code="a">TEST/1</subfield></datafield><datafield ind1=" " ind2=" " tag="245"><subfield code="a">title_1</subfield></datafield><datafield ind1=" " ind2=" " tag="700"><subfield code="a">1</subfield></datafield><datafield ind1=" " ind2=" " tag="980"><subfield code="a">BIB</subfield></datafield><datafield ind1=" " ind2=" " tag="FFT"><subfield code="a">https://mock_bucket.s3.amazonaws.com/1e50210a0202497fb79bc38b6ade6c34</subfield><subfield code="d">English</subfield><subfield code="n">TEST_1-EN.pdf</subfield></datafield></record><record><datafield ind1=" " ind2=" " tag="035"><subfield code="a">(DHL)2</subfield></datafield><datafield ind1=" " ind2=" " tag="245"><subfield code="a">title_2</subfield></datafield><datafield ind1=" " ind2=" " tag="700"><subfield code="a">2</subfield></datafield><datafield ind1=" " ind2=" " tag="980"><subfield code="a">BIB</subfield></datafield></record></collection>'
    
    dlx_dl.run(connect=db, type='bib', list=ids, output_file='STDOUT')
    assert diff_texts(capsys.readouterr().out, control) == []
    
def test_by_date(db, capsys):
    from xmldiff.main import diff_texts
    
    control = '<collection><record><datafield ind1=" " ind2=" " tag="035"><subfield code="a">(DHL)1</subfield></datafield><datafield ind1=" " ind2=" " tag="191"><subfield code="a">TEST/1</subfield></datafield><datafield ind1=" " ind2=" " tag="245"><subfield code="a">title_1</subfield></datafield><datafield ind1=" " ind2=" " tag="700"><subfield code="a">1</subfield></datafield><datafield ind1=" " ind2=" " tag="980"><subfield code="a">BIB</subfield></datafield><datafield ind1=" " ind2=" " tag="FFT"><subfield code="a">https://mock_bucket.s3.amazonaws.com/1e50210a0202497fb79bc38b6ade6c34</subfield><subfield code="d">English</subfield><subfield code="n">TEST_1-EN.pdf</subfield></datafield></record><record><datafield ind1=" " ind2=" " tag="035"><subfield code="a">(DHL)2</subfield></datafield><datafield ind1=" " ind2=" " tag="245"><subfield code="a">title_2</subfield></datafield><datafield ind1=" " ind2=" " tag="700"><subfield code="a">2</subfield></datafield><datafield ind1=" " ind2=" " tag="980"><subfield code="a">BIB</subfield></datafield></record></collection>'
    
    dlx_dl.run(connect=db, type='bib', modified_from=START.strftime('%Y-%m-%d'), output_file='STDOUT')
    assert diff_texts(capsys.readouterr().out, control) == []
    
    dlx_dl.run(connect=db, type='bib', modified_from=datetime.max.strftime('%Y-%m-%d'), output_file='STDOUT')
    assert capsys.readouterr().out == '<collection></collection>'
    
    dlx_dl.run(connect=db, type='bib', modified_within=100, output_file='STDOUT')
    assert diff_texts(capsys.readouterr().out, control) == []
    
    dlx_dl.run(connect=db, type='bib', modified_within=0, output_file='STDOUT')
    assert capsys.readouterr().out == '<collection></collection>'
    
@responses.activate
def test_post_and_log(db):
    from http.server import HTTPServer 
    from xmldiff.main import diff_texts
            
    server = HTTPServer(('127.0.0.1', 9090), None)
    responses.add(responses.POST, 'http://127.0.0.1:9090', body='test OK')
    dlx_dl.API_URL = 'http://127.0.0.1:9090'
    
    dlx_dl.run(connect=db, type='bib', modified_from=START.strftime('%Y-%m-%d'), api_key='x')
    
    entry = db['dummy']['dlx_dl_log'].find_one({'record_id': 1})
    assert entry['record_id'] == 1
    assert entry['response_code'] == 200
    assert entry['response_text'] == 'test OK'
    assert isinstance(entry['export_start'], datetime)
    assert isinstance(entry['time'], datetime)
    
    control = '<record><datafield ind1=" " ind2=" " tag="035"><subfield code="a">(DHL)1</subfield></datafield><datafield ind1=" " ind2=" " tag="191"><subfield code="a">TEST/1</subfield></datafield><datafield ind1=" " ind2=" " tag="245"><subfield code="a">title_1</subfield></datafield><datafield ind1=" " ind2=" " tag="700"><subfield code="a">1</subfield></datafield><datafield ind1=" " ind2=" " tag="980"><subfield code="a">BIB</subfield></datafield><datafield ind1=" " ind2=" " tag="FFT"><subfield code="a">https://mock_bucket.s3.amazonaws.com/1e50210a0202497fb79bc38b6ade6c34</subfield><subfield code="d">English</subfield><subfield code="n">TEST_1-EN.pdf</subfield></datafield></record>'
    
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
    control = '<record><datafield ind1=" " ind2=" " tag="035"><subfield code="a">(DHL)1</subfield></datafield><datafield ind1=" " ind2=" " tag="191"><subfield code="a">TEST/1</subfield></datafield><datafield ind1=" " ind2=" " tag="245"><subfield code="a">title_1</subfield></datafield><datafield ind1=" " ind2=" " tag="700"><subfield code="a">1</subfield></datafield><datafield ind1=" " ind2=" " tag="980"><subfield code="a">BIB</subfield></datafield></record>'

    dlx_dl.run(connect=db, type='bib', modified_from=START.strftime('%Y-%m-%d'), api_key='x')
    entry = db['dummy']['dlx_dl_log'].find_one({'record_id': 1})
    assert diff_texts(entry['xml'], control) == []
    