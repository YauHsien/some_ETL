import customer
import wic
from wic import RESTRICT
from wic.RESTRICT import (PCOFNSRAW, PCOFNGRAW, IMSCSFRAW, IMSHSSRAW, MADNHRRAW, MADODCRAW, IMSDRARAW, XMLNSSRAW, NOKOBWRAW, NOKOMWRAW, NOKIUMRAW, CM, FM, PM)
from wic import etl as ETL
from wic import db as DB
from wic.etl import key as SysKey
from wic.etl import column as Column
from wic.etl import ds as DataSource
from wic.etl import db as ETLDB
from wic.etl.etl_agent import ETLAgent
from wic import util as Util
from wic.util import ftp as FTP
from wic.util import folder as Folder
from wic.util import file as File
from wic.util.logging import logger
import datetime, pathlib, re, zipfile, csv, traceback



_re_zip1 = re.compile('^.+/((?!/).+)_(\d\d\d\d-\d\d-\d\d)[.]zip$')
_re_csv1 = re.compile('^(.+)_(\d\d\d\d-\d\d-\d\d)[.]txt$')



"""
## CAT: either RESTRICT.CO or RESTRICT.CM, RESTRICT.FM, or RESTRICT.PM
"""
def _download_zips(cusid, tech, CAT, ymd_dppath, ymd_cfgpath, limit= None, mod_name= __name__):

    ds = customer.get_default_DS_config_all(cusid, tech)
    reCAT = ds[RESTRICT.ZIP_FLT][CAT]
    flpath = ymd_cfgpath.joinpath('files.txt')
    logger(mod_name).info('checking "{}"...'.format(flpath))
    result = set()
    with open(str(flpath), 'r') as fo:
        c = 0
        for _, p in enumerate(fo):
            ftppath = pathlib.Path(p.rstrip())
            fn = ftppath.name
            zippath = ymd_dppath.joinpath(fn)
            m = reCAT.match(fn)
            #logger(__name__).debug((ftppath, fn, zippath, reCAT, m))
            if type(m) is type(re.match('', '')):
                FTP.download_binary(ds[RESTRICT.PROTOCOL], ds[RESTRICT.HOST], ds[RESTRICT.PORT],
                                    ds[RESTRICT.USER], ds[RESTRICT.PSWD],
                                    ftppath.as_posix(), str(zippath), mod_name)
                if CAT == RESTRICT.PM:
                    result.add((m.group(3), zippath))
                elif CAT in set([PCOFNSRAW, PCOFNGRAW, IMSCSFRAW, IMSHSSRAW, MADNHRRAW, MADODCRAW,
                                 IMSDRARAW, XMLNSSRAW, NOKOBWRAW, NOKOMWRAW, NOKIUMRAW]):
                    result.add((m.group(2), zippath))
                else:
                    result.add((m.group(1), zippath))

                if type(limit) is int:
                    c = c + 1
                    if c >= limit:
                        break
                    
        fo.close()
    return result



def _get_config(cusid, tech, date, mod_name):
    if type(date) is datetime.date:
        ymd = '{:%Y%m%d}'.format(date)
        cf = ETL.get_computed_config(cusid, tech, mod_name)
        sfx = str(pathlib.Path(cusid).joinpath(tech).joinpath(ymd))
        cfgbase = cf[RESTRICT.CONFIG_PATH].joinpath(cf[RESTRICT.CONFIG_FOLDER]).joinpath(sfx)
        dpbase = cf[RESTRICT.DATA_PATH].joinpath(sfx)
        return cfgbase, dpbase



def download_zips(cusid, tech, CAT, ymd_dppath, ymd_cfgpath, limit, mod_name):
    return _download_zips(cusid, tech, CAT, ymd_dppath, ymd_cfgpath, limit, mod_name)



def get_config(cusid, tech, date, mod_name):
    return _get_config(cusid, tech, date, mod_name)
    


def extract_info(cusid, tech, date, CAT, mod_name):
    if type(date) is datetime.date:
        cfgbase, dpbase = _get_config(cusid, tech, date, mod_name)
        LRC_z_s = _download_zips(cusid, tech, CAT, dpbase, cfgbase, mod_name= mod_name)
        reCAT = DataSource.get_computed_config(cusid, tech, mod_name)[RESTRICT.CSV_FLT][CAT]

        for LRC, zippath in LRC_z_s:
            zf = zipfile.ZipFile(str(zippath))
            infolist = zf.infolist()
            zf.close()
            for _, f in enumerate([ zi.filename
                                    for _, zi in enumerate(infolist)
                                    if reCAT.match(zi.filename) ]):
                yield (LRC, zippath, f)



def _get_recent_zip(zippath, date):
    if File.is_path(zippath) and File.exists(zippath) and type(date) is datetime.date:
        m = _re_zip1.match(zippath.as_posix())
        if m is not None:
            z = zippath.parent.parent.joinpath('{:%Y%m%d}/{}_{:%Y-%m-%d}.zip'.format(date, m.group(1), date))
            return z



def _get_recent_file(filename, date):
    if type(date) is datetime.date:
        m = _re_csv1.match(filename)
        if m is not None:
            return '{}_{:%Y-%m-%d}.txt'.format(m.group(1), date)
                


def extract_info_pair(date, date_to, cusid, tech, CAT, mod_name):
    if type(date) is datetime.date and type(date_to) is datetime.date:
        cfgbase1, dpbase1 = _get_config(cusid, tech, date,    mod_name)
        cfgbase2, dpbase2 = _get_config(cusid, tech, date_to, mod_name)
        
        LRC_z_s1, LRC_z_s2 = _download_zips(cusid, tech, CAT, dpbase1, cfgbase1, mod_name= mod_name),\
                             _download_zips(cusid, tech, CAT, dpbase2, cfgbase2, mod_name= mod_name)
        
        e = enumerate([ (l, z1, z2)
                        for _, (l, z1, z2) in enumerate([ (l, z, _get_recent_zip(z, date_to))
                                                          for _, (l, z) in enumerate(LRC_z_s1) ])
                        if (l, z2) in LRC_z_s2 ])
                    
        for _, (LRC, zippath1, zippath2) in e:
            
            zf1, zf2 = zipfile.ZipFile(str(zippath1)), zipfile.ZipFile(str(zippath2))
            infolist1, infolist2 = zf1.infolist(), zf2.infolist()
            zf1.close()
            zf2.close()

            e1 = enumerate([ (LRC, zippath1, zi.filename, zippath2, _get_recent_file(zi.filename, date_to))
                             for _, zi in enumerate(infolist1) ])

            fs = set([ zi.filename for _, zi in enumerate(infolist2) ])

            for _, t in enumerate([ (l, z1, f1, z2, f2) for _, (l, z1, f1, z2, f2) in e1 if f2 in fs ]):
                yield t

                

def _get_owner_table(cusid, tech, CAT, zippath, filename):
    conf = DataSource.get_computed_config(cusid, tech, __name__)
    re_zip = conf[RESTRICT.ZIP_FLT][CAT]
    re_csv = conf[RESTRICT.CSV_FLT][CAT]
    m_z = re_zip.match(str(zippath.name))
    if m_z is None:
        return None
    else:
        m_c = re_csv.match(filename)
        if m_c is None:
            return None
        else:
            if CAT == RESTRICT.CM or CAT == RESTRICT.CM:
                return 'CMDLTE', m_c.group(1)
            elif CAT == RESTRICT.FM:
                return 'FM', m_c.group(1)
            elif CAT == RESTRICT.PM:
                return m_z.group(1), m_c.group(1)


    
def extract_owner_tables(cusid, tech, date, CAT, extracted_info, mod_name):
    return [ o_t for _, o_t in enumerate([ _get_owner_table(cusid, tech, CAT, zippath, filename)
                                           for _, (_LRC, zippath, filename) in enumerate(extracted_info) ])
             if o_t is not None ]
        
                

def get_owner_tables(cusid, tech, date, CAT, mod_name):
    if type(date) is datetime.date:

        _dsconf = DataSource.get_computed_config(cusid, tech, mod_name)
        re_zip = _dsconf[RESTRICT.ZIP_FLT][CAT]
        re_csv = _dsconf[RESTRICT.CSV_FLT][CAT]
        
        result = set()
        for _, (z, f) in enumerate([ (zippath, filename)
                                     for _, zippath, filename in  extract_info(cusid, tech, date, CAT, mod_name)
                                     if re_zip.match(zippath.name) ]):

            if CAT == RESTRICT.CM:
                owner = 'CMDLTE'
            elif CAT == RESTRICT.FM:
                owner = 'FM'
            elif CAT in set([RESTRICT.PM, RESTRICT.PCOFNSRAW, RESTRICT.PCOFNGRAW, RESTRICT.IMSCSFRAW,
                             RESTRICT.IMSHSSRAW, RESTRICT.MADNHRRAW, RESTRICT.MADODCRAW, RESTRICT.IMSDRARAW,
                             RESTRICT.XMLNSSRAW, RESTRICT.NOKOBWRAW, RESTRICT.NOKOMWRAW,
                             RESTRICT.NOKIUMRAW]):
                m = re_zip.match(z.name)
                owner = m.group(1)

            m = re_csv.match(f)
            table = m.group(1)

            result.add((owner, table.upper()))

        return result


                
def _get_output_table_name(cusid, tech, CAT, filename):
    if CAT == 'CO':
        return 'COMMON_OBJECT'
    elif CAT == 'FM':
        return 'FX_ALARM'
    elif CAT in set([PCOFNSRAW, PCOFNGRAW, IMSCSFRAW, IMSHSSRAW, MADNHRRAW, MADODCRAW,
                     IMSDRARAW, XMLNSSRAW, NOKOBWRAW, NOKOMWRAW, NOKIUMRAW]):
        reCAT = DataSource.get_computed_config(cusid, tech, __name__)[RESTRICT.CSV_FLT][RESTRICT.PM]
        m = reCAT.match(filename)
        return m.group(1)
    else:
        reCAT = DataSource.get_computed_config(cusid, tech, __name__)[RESTRICT.CSV_FLT][CAT]
        m = reCAT.match(filename)
        return m.group(1)



def _extract_file(zippath, filename, path, mod_name):
    if File.is_path(zippath) and File.is_path(path):
        logger(mod_name).info('extract "{}"'.format(str(path.joinpath(filename))))
        zf = zipfile.ZipFile(str(zippath))
        zf.extract(filename, str(path))
        zf.close()



def extract_file(zippath, filename, target_folder_path, mod_name):
    _extract_file(zippath, filename, target_folder_path, mod_name)



def get_default_output_path(cusid, tech, date, CAT, filename, mod_name):
    _, dpbase = _get_config(cusid, tech, date, mod_name)
    tblname = _get_output_table_name(cusid, tech, CAT, filename)
    outfdrpath = dpbase.joinpath('{c}/{t}_{d:%Y%m}'.format(c= CAT, t= tblname, d= date))
    return outfdrpath.joinpath(filename)



def get_output_path(tblname, filename, cusid, tech, date, CAT, mod_name):
    _, dpbase = _get_config(cusid, tech, date, mod_name)
    outfdrpath = dpbase.joinpath(CAT).joinpath(tblname)
    return outfdrpath.joinpath(filename)
    


def perform_extraction(cusid, tech, date, CAT, LRC, zippath, filename, etl_agent= None, mod_name= __name__):
    if type(date) is datetime.date:
        cfgbase, dpbase = _get_config(cusid, tech, date, mod_name)
        tblname = _get_output_table_name(cusid, tech, CAT, filename)
        #outfdrpath = dpbase.joinpath(CAT).joinpath(tblname)
        #Folder.create(outfdrpath, mod_name)
        tpsfx = '{}.{}.{}.done'.format(CAT, LRC, filename)
        tagpath = cfgbase.joinpath('history').joinpath(tpsfx)
        workpath = dpbase.joinpath('tmp/{cat}'.format(cat= CAT))
        Folder.create(workpath, __name__)
        inpath = workpath.joinpath(filename)
        #outpath = outfdrpath.joinpath(filename)
        if not tagpath.exists():
            _extract_file(zippath, filename, workpath, mod_name)
            if type(etl_agent) is ETLAgent:
                etl_agent.approach(inpath)
                #logger(mod_name).debug(inpath.exists())
                File.remove(inpath, __name__)
                #tagpath.touch()
                


def update_columns(tech, date):

    if type(date) is datetime.date:
        logger(__name__).info('updating columns by {}'.format(str(date)))
        ymd = '{:%Y%m%d}'.format(date)
        cfgbase = wic.find_config_path(RESTRICT.CUSTOMER_ID, tech).joinpath(ymd)
        dpbase = wic.find_data_path(RESTRICT.CUSTOMER_ID, tech).joinpath(ymd)
        LRC_z_s = download_zips(RESTRICT.DB, tech, dpbase, cfgbase, __name__)
        put_LRCs(RESTRICT.DB, LRC_z_s, cfgbase, __name__)
        ps = [x for x in cfgbase.joinpath(RESTRICT.DB).glob('LRC*')]
        [p1, p2] = Util.pick(2, ps)
        good, bad = Column.build_columns(p1)
        ugly = Column.patch_columns(bad, p2)



def get_prefix_by_LRC(LRC):
    return SysKey.system_key[RESTRICT.CATEGORY][LRC, 'CTP']



def get_LRC_num(XRC):
    if XRC.startswith('LRC'):
        m = re.match('LRC(\d+)', XRC)
    if XRC.startswith('NHRC'):
        m = re.match('NHRC(\d+)', XRC)
    return int(m.group(1))



## get value in a line
def lnval(line, column):
    if type(line) is dict:
        if column in line:
            return line[column]



def _get_owner(term):
    if term == CM:
        return 'CMDLTE'
    elif term in set([FM, PM, PCOFNSRAW, PCOFNGRAW, IMSCSFRAW, IMSHSSRAW, MADNHRRAW,
                      MADODCRAW, IMSDRARAW, XMLNSSRAW, NOKOBWRAW, NOKOMWRAW, NOKIUMRAW]):
        return term

        
def _has_symbol(filepath):
    return File.exists(filepath)



def _put_symbol(filepath):
    if not File.exists(filepath):
        filepath.touch()



def _gen_SQL_list(SQL_gen_proc, owner, tblname, filepath, block_size= RESTRICT.MAX_ALLOWED_PACKET):
    if File.is_path(filepath):

        sqlformat = 'insert into {tblname} ({{columns}}) values {{values}};'.format(tblname= tblname)
        
        with open(str(filepath), 'r') as fo:
            reader = csv.DictReader(fo, delimiter= RESTRICT.DELIMITER)
            l, lines = len(sqlformat), list()
            for _, ln in enumerate(reader):
                if None in ln:
                    logger(__name__).debug('fuck')
                    logger(__name__).debug(ln)
                    for k in ln:
                        print(k, ln[k])
                if [ ln[k] for k in ln if ln[k] is None ] != []:
                    logger(__name__).debug('park')
                    logger(__name__).debug(ln)
                    for k in ln:
                        print(k, ln[k])
                #logger(__name__).debug(ln)
                l1 = sum([ n for n in map(lambda x: len(x) + 4, [ ln[k] for k in ln ])])
                #logger(__name__).debug(l1)
                if l + l1 >= block_size:
                    yield SQL_gen_proc(sqlformat, lines, owner, tblname)
                    del lines
                    l, lines = len(sqlformat) + l1, list([ln])
                else:
                    l = l + l1
                    lines.append(ln)
            if lines != list():
                yield SQL_gen_proc(sqlformat, lines, owner, tblname)
                del lines



def load(DDL_proc, SQL_gen_proc, date, cusid, tech, CAT, mod_name):
    """
    ## DDL_proc: lambda (owner, tblname) -> SQL
    ##- tblname: table name in database
    ## SQL_gen_proc: lambda (sqlformat, lines) -> SQL
    ## - sqlformat: 'insert into some_table ({columns}) values {values};'
    ## - lines: a list of { column: value }
    """
    if Util.is_function(DDL_proc) and Util.get_arity(DDL_proc) == 2 and\
       Util.is_function(SQL_gen_proc) and Util.get_arity(SQL_gen_proc) == 4:
        #import dispy
        cfgbase, dpbase = _get_config(cusid, tech, date, mod_name)
        #logger(mod_name).debug(cfgbase)
        #logger(mod_name).debug(dpbase)
        dbconf_base = ETLDB.get_computed_config(cusid, tech, mod_name)
        dbconf1 = dbconf_base[CAT]
        dbconfs = dict([
            (PCOFNSRAW, dbconf_base[PCOFNSRAW]),
            (PCOFNGRAW, dbconf_base[PCOFNGRAW]),
            (IMSCSFRAW, dbconf_base[IMSCSFRAW]),
            (IMSHSSRAW, dbconf_base[IMSHSSRAW]),
            (MADNHRRAW, dbconf_base[MADNHRRAW]),
            (MADODCRAW, dbconf_base[MADODCRAW]),
            (IMSDRARAW, dbconf_base[IMSDRARAW]),
            (XMLNSSRAW, dbconf_base[XMLNSSRAW]),
            (NOKOBWRAW, dbconf_base[NOKOBWRAW]),
            (NOKOMWRAW, dbconf_base[NOKOMWRAW]),
            (NOKIUMRAW, dbconf_base[NOKIUMRAW])
        ])
        hsformat = str(cfgbase.joinpath('history/{cat}.{tblname}.{filestem}.upload'))
        fdrpath = dpbase.joinpath(CAT)
        #logger(mod_name).debug(fdrpath)
        #cluster = dispy.JobCluster(_load_SQL)
        for p in fdrpath.glob('*/*.txt'):
            #logger(mod_name).debug('{} {}'.format(type(p), p))
            owner = _get_owner(p.parent.parent.name)
            tblname = p.parent.name
            hspath = pathlib.Path(hsformat.format(cat= CAT, tblname= tblname, filestem= p.stem))
            if _has_symbol(hspath):
                continue
            logger(__name__).debug('symbol create: "{}"'.format(hspath))
            #_put_symbol(hspath)
            if owner in dbconfs:
                dbconf = dbconfs[owner]
            else:
                dbconf = dbconf1
            ddl = DDL_proc(owner, tblname)
            conn = DB.get_connection(dbconf[RESTRICT.HOST],
                                     dbconf[RESTRICT.PORT],
                                     dbconf[RESTRICT.USER],
                                     dbconf[RESTRICT.PSWD],
                                     dbconf[RESTRICT.DB],
                                     dbconf[RESTRICT.CHARSET], mod_name)

            #if not DB.table_exists(conn, dbconf[RESTRICT.DB], tblname):
            #    block_size = DB.get_variable(conn, 'max_allowed_packet')
            #    DB.run_sql(conn, ddl)
            #DB.run_sql(conn, 'set autocommit=0;')
            ##conn.close()
            #for sql in _gen_SQL_list(SQL_gen_proc, owner, tblname, p, block_size):
            #    #logger(__name__).debug(sql)
            #    #conn = DB.get_connection(dbconf[RESTRICT.HOST],
            #    #                         dbconf[RESTRICT.PORT],
            #    #                         dbconf[RESTRICT.USER],
            #    #                         dbconf[RESTRICT.PSWD],
            #    #                         dbconf[RESTRICT.DB],
            #    #                         dbconf[RESTRICT.CHARSET], mod_name)
            #    #logger(__name__).debug(len(sql))
            #    DB.run_sql(conn, sql)
            #    #DB.run_sql(conn, 'commit;')
            #    #conn.close()
           
            ##conn = DB.get_connection(dbconf[RESTRICT.HOST],
            ##                         dbconf[RESTRICT.PORT],
            ##                         dbconf[RESTRICT.USER],
            ##                         dbconf[RESTRICT.PSWD],
            ##                         dbconf[RESTRICT.DB],
            ##                         dbconf[RESTRICT.CHARSET], mod_name)
            #DB.run_sql(conn, 'commit;')
            #conn.close()

            block_size = DB.get_variable(conn, 'max_allowed_packet')
            DB.run_sql(conn, ddl)
            try:
                cursor = conn.cursor()
                cursor.execute('start transaction')
                for i, sql in enumerate(_gen_SQL_list(SQL_gen_proc, owner, tblname, p, block_size)):
                    sql1 = re.sub(r'(\r\n|\n)', ' ', sql).strip()
                    sql2 = sql1[:512]
                    #logger(__name__).debug(sql1)
                    logger(__name__).debug('run: length {}; "{}{}"'.format(len(sql1), sql2, '...' if len(sql1) > len(sql2) else ''))
                    cursor.execute(sql)
                cursor.connection.commit()
            except Exception as ex:
                traceback.print_exc()
            finally:
                cursor.close()
                conn.close()
        


def just_run_sql(cusid, tech, CAT, SQL, mod_name= __name__):
    dbconf = ETLDB.get_computed_config(cusid, tech, mod_name)[CAT]
    conn = DB.get_connection(dbconf[RESTRICT.HOST],
                             dbconf[RESTRICT.PORT],
                             dbconf[RESTRICT.USER],
                             dbconf[RESTRICT.PSWD],
                             dbconf[RESTRICT.DB],
                             dbconf[RESTRICT.CHARSET], mod_name)
    #logger(mod_name).debug(SQL)
    DB.run_sql(conn, SQL)
    conn.close()
