import context
import wic.etl.util.ftp as FTP
import datetime

def test_generate_remote_files_Nothing():
    x = 'a'
    for i, ln in FTP.generate_remote_files('Nothing', datetime.datetime.now()):
        x = 'b'
    assert x == 'a'
