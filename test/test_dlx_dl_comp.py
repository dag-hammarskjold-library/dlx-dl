import pytest, responses
from moto import mock_s3
from datetime import datetime

@pytest.fixture
@mock_s3 # this has to go after the fixture decorator
def db():
    from dlx import DB
    from dlx.marc import Bib, Auth
    
    DB.connect('mongomock://localhost') # mock connection always creates a fresh db 
    
    Bib({'_id': 1}).set('998', 'z', '20200101000000').commit()
    Bib({'_id': 2}).set('998', 'z', '20200201000000').commit()
    Auth({'_id': 1}).set('998', 'z', '20200101000000').commit()
    
    return DB.client

@pytest.fixture
def excel_export():
    from tempfile import NamedTemporaryFile
    
    f = NamedTemporaryFile('r+')
    f.write('<html><table><tr><td>1</td><td>(DHL)1</td><td>20200101000000</td></tr><tr><td>2</td><td>(DHL)2</td><td>20200101000000</td></tr></table></html>')
    f.seek(0)
    
    return f
    
def test_run(db, excel_export, capsys):
    import dlx_dl_comp
    
    dlx_dl_comp.run(connect=db, file=excel_export.name, modified_from='2019-01-01', type='bib')
    assert capsys.readouterr().out == '2\t20200101000000\t20200201000000\n'
    
    dlx_dl_comp.run(connect=db, file=excel_export.name, modified_from='2019-01-01', type='auth')
    assert capsys.readouterr().out == '1\t0\t20200101000000\n'
    