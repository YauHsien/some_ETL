import wic
from wic import RESTRICT
from . import file as File
from .logging import logger
from . import logging as Logging
import sys, ftplib, datetime, re, json, time, socket, traceback, pathlib
from paramiko import Transport, SFTPClient



def _connect(protocol, host, port, user, password, path= None, mod_name= __name__):

    if protocol.upper() == RESTRICT.FTP:
        logger(mod_name).info('connecting FTP {}:{}'.format(host, port))
        ftp = ftplib.FTP()
        try:
            result = ftp.connect(host, port)
            logger(__name__).debug(result)
            result = ftp.login(user, password)
            logger(__name__).debug(result)
            if path is not None:
                result = ftp.cwd(path)
                logger(__name__).debug(result)
            ftp.sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            #ftp.sock.setsockopt(socket.IPPROTO_TCP. socket.TCP_KEEPINTVL, 75)
            #ftp.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 60)
            return ftp
        except Exception as e:
            logger(mod_name).error('bad FTP: {}'.format(e))

    if protocol.upper() == RESTRICT.SFTP:
        logger(mod_name).info('connecting SFTP {}:{}'.format(host, port))
        try:
            transport = Transport((host, port))
            transport.connect(username= user, password= password)
            sftp = SFTPClient.from_transport(transport)
            if path is not None:
                sftp.chdir(path)
            return sftp, transport
        except Exception as e:
            logger(mod_name).error('bad SFTP: {}'.format(e))
            traceback.print_exc()

        

def nlst(protocol, host, port, user, password, path, mod_name):

    if protocol.upper() == RESTRICT.FTP:
        logger(mod_name).info('connecting FTP {}:{}'.format(host, port))
        ftp = ftplib.FTP()
        try:
            result = ftp.connect(host, port)
            logger(__name__).debug(result)
            result = ftp.login(user, password)
            logger(__name__).debug(result)
            logger(mod_name).info('try to fecth {}'.format(path))
            result = ftp.nlst(path)
            #logger(__name__).debug(result)
            logger(mod_name).info('{} record{} fetched'.format(len(result), '' if len(result) < 2 else 's'))
            ftp.close()
            return result
        except Exception as e:
            logger(mod_name).error('bad FTP: {}'.format(e))
            traceback.print_exc()
            return []

    elif protocol.upper() == RESTRICT.SFTP:
        logger(mod_name).info('connecting FTP {}:{}'.format(host, port))
        sftp, transport = _connect(protocol, host, port , user, password, path, __name__)
        logger(mod_name).info('try to fecth {}'.format(path))
        try:
            p = sftp.getcwd()
            result = [ '{}/{}'.format(p, fp) for fp in sftp.listdir() ]
            logger(mod_name).info('{} record{} fetched'.format(len(result), '' if len(result) < 2 else 's'))
            sftp.close()
            transport.close()
            return result
        except Exception as e:
            logger(mod_name).error('bad SFTP: {}'.format(e))
            traceback.print_exc()
            return []

    else:
        logger(mod_name).warning('ignoring {} {}:{}'.format(protocol, host, port))
        return []


    
def print_progress(fo, fsize):
    Logging.put_progress(fo.tell(), fsize,
                         '{:.2f} / {:.2f} MBs ({:.1f}%)'.format(
                             fo.tell() / RESTRICT.MB_TO_BYTES,
                             fsize / RESTRICT.MB_TO_BYTES,
                             fo.tell() / fsize * 100))

    

def _ftp_progress(ftp= None, fo= None, fsize= None, bsize= None, state= None, mod_name= __name__):

    def write(data, ftp, fo, fsize, bsize, state, mod_name):
        fo.write(data)
        if bsize == 0 or state[0] % bsize == 0 or fo.tell() == fsize:
            state[0] = 0
            print_progress(fo, fsize)
            if ftp is not None:
                ftp.voidcmd('NOOP')
        state[0] = state[0] + 1

    return lambda data: write(data, ftp, fo, fsize, bsize, state, mod_name)



def download_binary(protocol, host, port, user, password, src, target, mod_name):

    if File.exists(target) and File.check_zipfile(target, __name__):
        return None

    if protocol.upper() ==  RESTRICT.FTP:

        ftp = _connect(protocol, host, port, user, password, mod_name= mod_name)
        if ftp is None:
            return None
        
        fsize = ftp.size(src)
        ftpcmd = 'RETR {file}'.format(file= src)
        with open(target, 'wb') as fo:
            logger(__name__).info('downloading \"{}\"...'.format(src))
            percent = list([1])
            bsize = int(fsize / RESTRICT.BLOCKSIZE / 4) if fsize > RESTRICT.BLOCKSIZE * 3 else 16
            logger(__name__).debug('progress reporting: per {} block{}'.format(bsize, '' if bsize < 2 else 's'))
            try:
                Logging.switch_to_progress(__name__)
                print_progress(fo, fsize)
                ftp.voidcmd('TYPE I')
                result = ftp.retrbinary(ftpcmd, _ftp_progress(ftp, fo, fsize, bsize, percent, __name__), RESTRICT.BLOCKSIZE)
                Logging.switch_to_normal(__name__)
                logger(__name__).debug(result)
            except Exception as e:
                Logging.switch_to_normal(__name__)
                logger(__name__).debug('cur {}: {}'.format(type(e), e))
                traceback.print_exc()
            fo.close()
            
        try:
            ftp.quit()
        except Exception as e:
            logger(__name__).warning('bad FTP: quit; {} {}'.format(type(e), e))
            try:
                ftp.close()
            except:
                pass

    if protocol.upper() == RESTRICT.SFTP:

        sftp, transport = _connect(protocol, host, port, user, password, mod_name= mod_name)
        if sftp is None:
            return None

        #logger(__name__).debug(src)
        fsize = sftp.stat(src).st_size
        ftpcmd = 'RETR {file}'.format(file= src)
        with open(target, 'wb') as fo:
            logger(__name__).info('downloading \"{}\"...'.format(src))
            percent = list([1])
            bsize = int(fsize / RESTRICT.BLOCKSIZE / 4) if fsize > RESTRICT.BLOCKSIZE * 3 else 16
            #sftp.setblock(bsize)
            logger(__name__).debug('progress reporting: per {} block{}'.format(bsize, '' if bsize < 2 else 's'))
            Logging.switch_to_progress(__name__)
            print_progress(fo, fsize)
            try:
                sftp.getfo(src, fo, lambda n, m: _ftp_progress(fo= fo, fsize= fsize, bsize= bsize, state= percent, mod_name= __name__))
                Logging.switch_to_normal(__name__)
            except Exception as e:
                Logging.switch_to_normal(__name__)
                logger(__name__).debug('cur {}: {}'.format(type(e), e))
                traceback.print_exc()

        sftp.close()
        sftp.close()
        
    File.check_zipfile(target, __name__)
