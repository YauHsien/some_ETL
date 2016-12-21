import customer
from wic import RESTRICT
from wic.RESTRICT import (PCOFNSRAW, PCOFNGRAW, IMSCSFRAW, IMSHSSRAW, MADNHRRAW, MADODCRAW, IMSDRARAW, XMLNSSRAW, NOKOBWRAW, NOKOMWRAW, NOKIUMRAW)
from wic import etl as ETL
from wic.etl import key as SysKey
from wic.etl import common as Common
from wic.etl import ds as DataSource
from wic.etl.ds import ds_column as DSColumn
from wic.etl import db as ETLDB
from wic.etl.etl_agent import ETLAgent
from wic.etl.line_extractor import LineExtractor
from wic.etl import db as DB
from wic.util import file as File
from wic.util import folder as Folder
from wic.util.logging import logger
import datetime, pathlib, re



_cat = RESTRICT.PM
_re_tblname_with_ym = re.compile('^(.+)_\d\d\d\d\d\d$')



def extract_columns(cusid, tech, date):
    Common.extract_columns(cusid, tech, _cat, date, __name__)



def _get_table_name(filename, csv_flt):
    m = csv_flt.match(filename)
    tblname = m.group(1)
    return tblname


    
#def _get_input_header(tblcols, tblname):
#    #logger(__name__).debug(tblcols.keys())
#    cols = tblcols[tblname]
#    return dict([ (k, cols[k]['type']) for k in cols ])



def _get_li_ex_header(header):
    header['_id'] = 'bigint'
    header['LRC'] = 'int'
    header['TIME'] = 'datetime'
    return header



def _get_line_proc(prefix, LRC, id_col, date):
    prefix1 = str(prefix)
    LRCn = str(Common.get_LRC_num(LRC))
    #TIME = '{:%Y-%m-%d}'.format(date)
    ext_cols = set(['_id', 'LRC', 'TIME'])

    def _get_line_proc(header, line, linum, prefix, LRCn):
        cols = set(line.keys())
        wanted = set(header.keys())

        line['_id'] = prefix + line[id_col]
        line['LRC'] = LRCn
        line['TIME'] = line['PERIOD_START_TIME']

        for _, k in enumerate(cols):
            if k not in wanted:
                line.pop(k)

        return line

    return lambda header, line, linum: _get_line_proc(header, line, linum, prefix1, LRCn)



"""
## return: (prefix, owner, tblname, id_col, agg_terms)
## - prefix
## - id_col
## - agg_terms
"""
def _do_pm_mapping(LRC, owner, tblname):

    if owner == 'PCOFNSRAW': ## (1)
        return _do_PCOFNSRAW_mapping(LRC, owner, tblname)

    if owner == 'PCOFNGRAW': ## (2)
        return _do_PCOFNGRAW_mapping(LRC, owner, tblname)

    if owner == 'IMSCSFRAW': ## (3)
        return _do_IMSCSFRAW_mapping(LRC, owner, tblname)

    if owner == 'IMSHSSRAW': ## (4)
        return _do_IMSHSSRAW_mapping(LRC, owner, tblname)

    if owner == 'MADNHRRAW': ## (5)
        return _do_MADNHRRAW_mapping(LRC, owner, tblname)

    if owner == 'MADODCRAW': ## (6)
        return _do_MADODCRAW_mapping(LRC, owner, tblname)

    if owner == 'IMSDRARAW': ## (7)
        return _do_IMSDRARAW_mapping(LRC, owner, tblname)

    if owner == 'XMLNSSRAW': ## (8)
        return _do_XMLNSSRAW_mapping(LRC, owner, tblname)

    if owner == 'NOKOBWRAW': ## (9)
        return _do_NOKOBWRAW_mapping(LRC, owner, tblname)

    if owner == 'NOKOMWRAW': ## (10)
        return _do_NOKOMWRAW_mapping(LRC, owner, tblname)

    if owner == 'NOKIUMRAW': ## (11)
        return _do_NOKIUMRAW_mapping(LRC, owner, tblname)



def get_pm_info(LRC, owner, tblname):
    return _do_pm_mapping(LRC, owner, tblname)



"""
## yield: (LRC, zippath, filename, prefix, owner, tblname, id_col, agg_terms)
## - LRC
## - zippath
## - filename
## - prefix
## - owner
## - tblname
## - id_col
## - agg_terms
"""
def _extract_PM(cusid, tech, date, CAT= _cat):
    if type(date) is datetime.date:

        dsconfig = DataSource.get_computed_config(cusid, tech, __name__)
        pm_zflt, pm_cflt = dsconfig[RESTRICT.ZIP_FLT][CAT], dsconfig[RESTRICT.CSV_FLT][CAT]
        
        ei = Common.extract_info(cusid, tech, date, CAT, __name__)
        for i, (LRC, zippath, filename) in enumerate(ei):

            owner, tblname = pm_zflt.match(zippath.name).group(1), pm_cflt.match(filename).group(1)
            thing = _do_pm_mapping(LRC, owner, tblname)
            if thing is not None:
                prefix, id_col, agg_terms = thing
                yield LRC, zippath, filename, prefix, owner, tblname, id_col, agg_terms


"""
#def _get_daily_agg_proc(prefix, LRC, date, agg_rules):
#    prefix1 = str(prefix)
#    LRCn = str(Common.get_LRC_num(LRC))
#    TIME = '{:%Y-%m-%d}'.format(date)
#    ext_cols = set(['_id', 'LRC', 'TIME'])
#    
#    return lambda header, line, linum: _get_daily_agg_proc(header, line, linum, prefix1, LRCn)
"""             


def extract(cusid, tech, date, CAT= _cat):
    if type(date) is datetime.date:
        logger(__name__).info('updating {} PM'.format(str(date)))

        owner_tables = Common.get_owner_tables(cusid, tech, date, CAT, __name__)
        #tblcols = DSColumn.extract_columns(cusid, tech, date, CAT, owner_tables, __name__)
        LRC_ow_ta_columns = DSColumn.get_all_columns(cusid, tech, date, _cat, __name__)
        dsconf = customer.get_default_DS_config_all(cusid, tech)

        ei = _extract_PM(cusid, tech, date, CAT)
        for i, (LRC, zippath, filename, prefix, owner, tblname, id_col, agg_terms) in enumerate(ei):
            """
            Common.perform_extraction(cusid, tech, date, CAT, LRC, zippath, filename, None, __name__)
            """
            tblcols = LRC_ow_ta_columns[LRC][owner, tblname]
            etl_agent = ETLAgent(tblcols, dict(), delimiter= RESTRICT.DELIMITER, newline= None, mod_name= __name__)
            outpath = Common.get_default_output_path(cusid, tech, date, owner, filename, __name__)
            Folder.create(outpath.parent, __name__)
            ex_header = _get_li_ex_header(tblcols)
            line_proc = _get_line_proc(prefix, LRC, id_col, date)
            lineExtr = LineExtractor(ex_header, outpath, RESTRICT.DELIMITER, line_proc, __name__)
            """
            #agg_rules = ETLDB.get_agg_rules(cusid, tech, owner, tblname, __name__)
            #daily_agg_proc = _get_daily_agg_proc(prefix, LRC, date, agg_rules['all'])
            #time_agg_proc =          ...  agg_rules['all']
            #obj_agg_proc =           ... agg_rules['all']
            #repAggList =
            #rep_agg_proc =
            #dailyAgg = Aggregator(ex_header, daOutpath, RESTRICT.DELIMITER, daily_agg_proc, __name__)
            #timeAgg = Aggregator(ex_header, taOutpath, RESTRICT.DELIMITER, time_agg_proc, __name__)
            #objAgg = Aggregator(ex_header, oaOutpath, RESTRICT.DELIMITER, obj_agg_proc, __name__)
            #repAggList = [ Aggregator(ex_header, raOutpath, RESTRICT.DELIMITER, rep_agg_proc, __name__)
            #               for _, (raLevel, raOutpath, rep_agg_proc) in enumerate(repAggList) ]
            """
            etl_agent.add(lineExtr, 'default')
            """
            #etl_agent.add(dailyAgg, 'daily aggregation')
            #etl_agent.add(timeAgg, 'time aggregation')
            #etl_agent.add(objAgg, 'object aggregation')
            #for i, repAgg in enumerate(repAggList):
            #    etl_agent.add(repAgg, 'report aggregation #{}'.format(i + 1))
            """
            Common.perform_extraction(cusid, tech, date, CAT, LRC, zippath, filename, etl_agent, __name__)
            etl_agent.clean()
            del etl_agent



def chkcol(cusid, tech, date, load= False):
    if type(date) is datetime.date:
        logger(__name__).info('checking column {} PM'.format(str(date)))
        ymd = '{:%Y%m%d}'.format(date)
        dppath = ETL.get_computed_config(cusid, tech, __name__)[RESTRICT.DATA_PATH]
        
        database = DB.get_computed_config(cusid, tech, __name__)[RESTRICT.PM][RESTRICT.DB]
        advpath = dppath.joinpath('{}/{}/{}/columns/check/{}.sql'.format(cusid, tech, ymd, database))
        File.remove(advpath, __name__)

        owner_tables = Common.get_owner_tables(cusid, tech, date, _cat, __name__)
        cols = DSColumn.extract_columns(cusid, tech, date, _cat, owner_tables, __name__)

        for tblname1 in cols:

            tblname = '{}_{:%Y%m}'.format(tblname1, date)
            tblnameLatest = '{}_latest'.format(tblname1)
            dbcols = DB.get_columns(cusid, tech, RESTRICT.PM, tblname, __name__)

            new = dict()
            add = dict()
            alter = dict()
            
            if dbcols == dict():
                new[tblname] = cols[tblname1]
            
            else:
                for col in cols[tblname1]:

                    if col not in dbcols:
                        add[col] = cols[tblname1][col]
                        
                    elif not DSColumn.type_equal(cols[tblname1][col], dbcols[col]):
                        #logger(__name__).debug(cols[tblname1][col])
                        #logger(__name__).debug(dbcols[col])
                        alter[col] = cols[tblname1][col]

            if new != dict() or add != dict() or alter != dict():

                profile = list()
                if new != dict():
                    profile.append('create')
                if add != dict():
                    profile.append('add column')
                if alter != dict():
                    profile.append('change column')
                logger(__name__).info('PM table {}: {}'.format(tblname, profile))

                advpath.touch()
                with open(str(advpath), 'a') as fo:
                    
                    fo.write('use {};\n'.format(database))
                    
                    if new != dict():
                        sql = DSColumn.to_sql(create= new)
                        if load:
                            Common.just_run_sql(cusid, tech, _cat, sql, __name__)
                        fo.write('{};\n'.format(sql))
                        
                    if add != dict():
                        fo.write('{};\n'.format(DSColumn.to_sql(tblname, add_column= add)))

                    if alter != dict():
                        fo.write('{};\n'.format(DSColumn.to_sql(tblname, change_column= alter)))
                        
                    fo.close()

        if advpath.exists():
            logger(__name__).info('advice: "{}"'.format(str(advpath)))



"""
## return: (prefix, id_col, agg_terms) or None
## - prefix
## - id_col
## - agg_terms
"""
def _do_PCOFNSRAW_mapping(LRC, owner, tblname): ## (1)
            
    prefix = SysKey.system_key[RESTRICT.CATEGORY][LRC, RESTRICT.CTP]
    id_col = 'FINS_ID'
    if tblname.upper().endswith('-TA'):
        agg_terms = [RESTRICT.PLMN, RESTRICT.FINS, RESTRICT.FINS_TA]
    else:
        agg_terms = [RESTRICT.PLMN, RESTRICT.FINS]
    return prefix, id_col, agg_terms



"""
## return: (prefix, id_col, agg_terms) or None
## - prefix
## - id_col
## - agg_terms
"""
def _do_PCOFNGRAW_mapping(LRC, owner, tblname): ## (2)

    prefix = SysKey.system_key[RESTRICT.CATEGORY][LRC, RESTRICT.CTP]
    id_col = 'FING_ID'
    agg_terms = [RESTRICT.PLMN, RESTRICT.FING]
    return prefix, id_col, agg_terms



"""
## return: (prefix, id_col, agg_terms) or None
## - prefix
## - id_col
## - agg_terms
"""
def _do_IMSCSFRAW_mapping(LRC, owner, tblname): ## (3)

    prefix = SysKey.system_key[RESTRICT.CATEGORY][LRC, RESTRICT.CTP]
    tag = re.match('^.+_(\w+)_RAW$', tblname, flags= re.IGNORECASE).group(1).upper()
    id_col = None
    
    if tag == 'DIAMDISP':
        id_col = 'DIAMETERDISP_ID'
        agg_terms = [RESTRICT.PLMN]
    elif tag == 'ICSCF':
        id_col = 'ICSCF_ID'
        agg_terms = [RESTRICT.PLMN, RESTRICT.CSCF]
    elif tag in set(['CX', 'SH']):
        id_col = 'COMMON_ID'
        agg_terms = [RESTRICT.PLMN]
    elif tag in set(['BRM', 'BC', 'CPU', 'EATF', 'FS', 'NET', 'RF', 'RO']):
        id_col = 'CSCF_ID'
        agg_terms = [RESTRICT.PLMN, RESTRICT.CSCF]
    elif tag in set(['BGCF', 'CFRAME', 'DIA', 'DNSPH', 'HTTPCL', 'HTTPSV', 'IBCF', 'MCF', 'NM', 'OVLCT', 'PCSCF', 'SCSCF']):
        id_col = '{}_ID'.format(tag)
        agg_terms = [RESTRICT.PLMN]
    
    if id_col is not None:
        return prefix, id_col, agg_terms



"""
## return: (prefix, id_col, agg_terms) or None
## - prefix
## - id_col
## - agg_terms
"""
def _do_IMSHSSRAW_mapping(LRC, owner, tblname): ## (4)

    prefix = SysKey.system_key[RESTRICT.CATEGORY][LRC, RESTRICT.CTP]
    tag = re.match('^.+_(\w+)_RAW$', tblname, flags= re.IGNORECASE).group(1).upper()
    id_col = None

    mapping = dict([ ('ACTPADA1', 'ACTPADAPTER_ID'),
                     ('DIAMDISP', 'DIAMETERDISP_ID'),
                     ('CX',       'COMMON_ID'),
                     ('S6A',      'COMMON_ID'),
                     ('SH',       'COMMON_ID'),
                     ('SWX',      'COMMON_ID'),
                     ('BRM',      'HSSFE_ID'),
                     ('CPU',      'HSSFE_ID'),
                     ('FS',       'HSSFE_ID'),
                     ('NET',      'HSSFE_ID') ])

    if tag in mapping:
        id_col = mapping[tag]
        if tag in set(['CPU', 'FS', 'NET' ]):
            agg_terms = [RESTRICT.PLMN, RESTRICT.HSSFE]
        else:
            agg_terms = [RESTRICT.PLMN]
    elif tag in set(['AN2', 'ARQ', 'ATI', 'CFRAME', 'DIA', 'DNSPH', 'HTTPCL', 'HTTPSV', 'LCR', 'LUP', 'MTC', 'ORREQ']):
        id_col = '{}_ID'.format(tag)
        agg_terms = [RESTRICT.PLMN]
    
    if id_col is not None:
        return prefix, id_col, agg_terms



"""
## return: (prefix, id_col, agg_terms) or None
## - prefix
## - id_col
## - agg_terms
"""
def _do_MADNHRRAW_mapping(LRC, owner, tblname): ## (5)

    prefix = SysKey.system_key[RESTRICT.CATEGORY][LRC, RESTRICT.CTP]
    tag = re.match('^.+_(\w+)_RAW$', tblname, flags= re.IGNORECASE).group(1).upper()
    id_col = None

    mapping = dict([ ('ACADAP',   'ACTPADAPTER_ID'),
                     ('SCI',      'DDH_ID'),
                     ('NET',      'NTHLRFE_ID'),
                     ('HSM',      'UTMCHSM_ID') ])

    if tag in mapping:
        id_col = mapping[tag]
        agg_terms = [RESTRICT.PLMN]
    elif tag in set(['BRM', 'CPU', 'FS']):
        id_col = 'NTHLRFE_ID'
        agg_terms = [RESTRICT.PLMN, RESTRICT.NTHLRFE]
    elif tag in set(['M3UALINK', 'SS7STAT', 'SCTP']):
        id_col = 'SS7STAT_ID'
        agg_terms = [RESTRICT.PLMN]
    elif tag in set(['CLRPM', 'DNSPH', 'NOR', 'PUR', 'SHH', 'URL']):
        id_col = 'LTE_ID'
        agg_terms = [RESTRICT.PLMN]
    elif tag in set(['AFW', 'CBM', 'CBS', 'CFRAME', 'DIA', 'DIAMDISP', 'HDL', 'HTTPCL', 'HTTPSV', 'IDR', 'MTC', 'LUP', 'NAIR', 'OVLD', 'RSR', 'SAI', 'SDM', 'SLA', 'SMS', 'TFR', 'TPLCS', 'TPSIS', 'USD', 'TMCHSM', 'SYCTRAC']):
        id_col = '{}_ID'.format(tag)
        agg_terms = [RESTRICT.PLMN]
    
    if id_col is not None:
        return prefix, id_col, agg_terms



"""
## return: (prefix, id_col, agg_terms) or None
## - prefix
## - id_col
## - agg_terms
"""
def _do_MADODCRAW_mapping(LRC, owner, tblname): ## (6)

    prefix = SysKey.system_key[RESTRICT.CATEGORY][LRC, RESTRICT.CTP]
    tag = re.match('^.+_(\w+)_RAW$', tblname, flags= re.IGNORECASE).group(1).upper()
    id_col = None

    if tag in set(['DIRN1', 'HOST', 'NTFN', 'PGWN', 'TENANT']):
        id_col = '{}_ID'.format(tag)
        agg_terms = [RESTRICT.PLMN]
    
    if id_col is not None:
        return prefix, id_col, agg_terms



"""
## return: (prefix, id_col, agg_terms) or None
## - prefix
## - id_col
## - agg_terms
"""
def _do_IMSDRARAW_mapping(LRC, owner, tblname): ## (7)

    prefix = SysKey.system_key[RESTRICT.CATEGORY][LRC, RESTRICT.CTP]
    tag = re.match('^.+_(\w+)_RAW$', tblname, flags= re.IGNORECASE).group(1).upper()
    id_col = None

    if tag == 'DIAMDISP':
        id_col = 'DIAMETERDISP_ID'
        agg_terms = [RESTRICT.PLMN]
    elif tag == 'OVLD':
        id_col = 'OVERLOADHANDLING_ID'
        agg_terms = [RESTRICT.PLMN]
    elif tag in set(['BRM', 'CPU', 'DRA', 'FS', 'NET']):
        id_col = 'DRA_ID'
        agg_terms = [RESTRICT.PLMN, RESTRICT.DRA]
    elif tag in set(['CFRAME', 'DIA', 'DNSPH', 'HTTPCL', 'HTTPSV']):
        id_col = '{}_ID'.format(tag)
        agg_terms = [RESTRICT.PLMN]
    
    if id_col is not None:
        return prefix, id_col, agg_terms


    
"""
## return: (prefix, id_col, agg_terms) or None
## - prefix
## - id_col
## - agg_terms
"""
def _do_XMLNSSRAW_mapping(LRC, owner, tblname): ## (8)

    prefix = SysKey.system_key[RESTRICT.CATEGORY][LRC, RESTRICT.CTP]
    tag = re.match('^.+_(\w+)_RAW$', tblname, flags= re.IGNORECASE).group(1).upper()
    id_col = None

    if tag == 'HLR':
        id_col = 'HLR_ID'
        agg_terms = [RESTRICT.PLMN, RESTRICT.HLR]
    elif tblname in set(['RNS_PS_CLOM_UNIT2_RAW', 'RNS_PS_SIP_UNIT2_RAW', 'RNS_PS_CCMEA_CC1_RAW', 'RNS_PS_SERVI_SERVICE1_RAW']) or tag in set(['MSC', 'LAC1', 'ANN1', 'CGRDIR1', 'UPD2', 'DESTNAME1', 'DIPRSP1', 'ASSOIND1', 'ASSOSET1', 'FEAC1', 'MGWIP1', 'TRA1', 'TCAT1']):
        id_col = 'MSC_ID'
        agg_terms = [RESTRICT.PLMN, RESTRICT.MSC]
    
    if id_col is not None:
        return prefix, id_col, agg_terms


    
"""
## return: (prefix, id_col, agg_terms) or None
## - prefix
## - id_col
## - agg_terms
"""
def _do_NOKOBWRAW_mapping(LRC, owner, tblname): ## (9)

    prefix = SysKey.system_key[RESTRICT.CATEGORY][LRC, RESTRICT.CTP]
    id_col = 'OBGW_ID'
    agg_terms = [RESTRICT.PLMN, RESTRICT.OBGW]
    return prefix, id_col, agg_terms



"""
## return: (prefix, id_col, agg_terms) or None
## - prefix
## - id_col
## - agg_terms
"""
def _do_NOKOMWRAW_mapping(LRC, owner, tblname): ## (10)

    prefix = SysKey.system_key[RESTRICT.CATEGORY][LRC, RESTRICT.CTP]
    id_col = 'OMGW_ID'
    agg_terms = [RESTRICT.PLMN, RESTRICT.OMGW]
    return prefix, id_col, agg_terms



"""
## return: (prefix, id_col, agg_terms) or None
## - prefix
## - id_col
## - agg_terms
"""
def _do_NOKIUMRAW_mapping(LRC, owner, tblname): ## (11)

    prefix = SysKey.system_key[RESTRICT.CATEGORY][LRC, RESTRICT.UTP]
    tag = re.match('^.+_(\w+)_RAW$', tblname, flags= re.IGNORECASE).group(1).upper()
    id_col = None

    t = dict([ ('id_col',    'INUMSERVER_ID'),
               ('agg_terms', [RESTRICT.PLMN,
                              RESTRICT.INUMSERVER]) ])
    mapping = dict([ ('NMSRV',    dict([ ('id_col',    'INUMIWF_ID'),
                                         ('agg_terms', [RESTRICT.PLMN,
                                                        RESTRICT.INUMSERVER_INUMIWF]) ])),
                     ('NMSRV1',   dict([ ('id_col',    'INUM_ID'),
                                         ('agg_terms', [RESTRICT.PLMN,
                                                        RESTRICT.INUMSERVER_INUM]) ])),
                     ('BPM', t),
                     ('CPU', t),
                     ('FS',  t),
                     ('NET', t) ])

    if tag in mapping:
        id_col =    mapping[tag]['id_col']
        agg_terms = mapping[tag]['agg_terms']
    
    if id_col is not None:
        return prefix, id_col, agg_terms



def _format_columns(columns):
    colformat, valformat = '_id,LRC,TIME', "{_id},{LRC},'{TIME}'"
    type1 = set(['VARCHAR2', 'DATE', 'DATETIME'])
    for _, cn in enumerate(columns):
        colformat = colformat + ',{cname}'.format(cname= cn)
        if columns[cn]['type'].upper() in type1:
            valformat = valformat + ",'{{{cname}}}'".format(cname= cn)
        else:
            valformat = valformat + ',{{{cname}}}'.format(cname= cn)
    return colformat, '({})'.format(valformat)



def _tblname_without_ym(tblnameWithYm):
    m = _re_tblname_with_ym.match(tblnameWithYm)
    if m is not None:
        return m.group(1)


def get_DDL_proc(owner_table_columns):
    if type(owner_table_columns) is dict:
        return lambda owner, tblname:\
            DSColumn._to_DDL_create(dict([ (tblname, owner_table_columns[owner, _tblname_without_ym(tblname)]) ]))



def get_SQL_gen_proc(owner_table_columns):
    if type(owner_table_columns) is dict:

        c = dict([ ((owner, tblname), _format_columns(owner_table_columns[owner, tblname]))
                   for _, (owner, tblname) in enumerate(owner_table_columns) ])
        
        def _SQL_gen_proc(sqlformat, lines, owner, tblname):
            """
            ## sqlformat: 'insert into some_table ({columns}) values {values};'
            ## lines: a list of { column: value }
            """
            cs, valformat = c[owner, _tblname_without_ym(tblname)]
            vss = set()
            for _, ln in enumerate(lines):
                for _, k in enumerate(ln):
                    if ln[k] == '':
                        ln[k] = 'default'
                vs = valformat.format(**ln)
                vss.add(vs)
            vss = ','.join(vss)
            return sqlformat.format(columns = cs, values= vss)

        return lambda sqlformat, lines, owner, tblname:\
            _SQL_gen_proc(sqlformat, lines, owner, tblname)



def load(cusid, tech, date):
    _g = DSColumn.gen_all_columns(cusid, tech, date, _cat, __name__)
    c = dict([ ((owner, tblname), columns) for owner, tblname, columns in _g ])
    _DDL_proc = get_DDL_proc(c)
    _SQL_gen_proc = get_SQL_gen_proc(c)
    for CAT in [PCOFNSRAW, PCOFNGRAW, IMSCSFRAW, IMSHSSRAW, MADNHRRAW, MADODCRAW,
                IMSDRARAW, XMLNSSRAW, NOKOBWRAW, NOKOMWRAW, NOKIUMRAW]:
        Common.load(_DDL_proc, _SQL_gen_proc, date, cusid, tech, CAT, __name__)



def get_truncate_latest_proc(date):
    return lambda owner, tblname:\
        'drop table if exists {table}_latest; create table {table}_latest like {table}_{d:%Y%m}'.format(table= _tblname_without_ym(tblname), d= date)

    
    
def get_copy_latest_proc(date):
    return lambda sqlformat, lines, owner, tblname:\
        "insert into {table}_latest select * from {table}_{d:%Y%m} where TIME = '{d:%Y-%m-%d}' ".format(table= _tblname_without_ym(tblname), d= date)
    


def load_latest(cusid, tech, date):
    _g = DSColumn.gen_all_columns(cusid, tech, date, _cat, __name__)
    c = dict([ ((owner, tblname), columns) for owner, tblname, columns in _g ])
    _DDL_proc = get_truncate_latest_proc(date)
    _SQL_gen_proc = get_copy_latest_proc(date)
    Common.load(_DDL_proc, _SQL_gen_proc, date, cusid, tech, _cat, __name__)
    
