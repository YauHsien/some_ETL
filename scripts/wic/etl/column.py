import wic
from wic import RESTRICT
from wic import db as DB
from wic import util as Util
from wic.etl import db as DB1
from wic.util import file as File
from wic.etl.key import system_key


_reDB = system_key['DATASOURCE']['CSV_FLT']['DB']

def build_columns(path):

    if File.is_path(path):
        good, bad = dict(), dict()
        File.gen_file_lines(path, newline= '\r\n', encoding= 'latin-1')
        pass



def patch_columns(bad, path):

    if type(bad) is dict() and File.is_path(path):
        ugly = dict()
        File.gen_file_lines(path, newline= '\r\n', encoding= 'latin-1')
        pass



def simple_extractor(column):

    def _extract(column, line):
        if type(line) is dict and column in line:
            return line[column]

    return lambda line: _extract(column, line)



def custom_extractor(column, callback):

    if Util.is_function(callback) and Util.get_arity(f) == 2:
        return lambda line: callback(column, line)



def find_file_path(cusid, tech, CAT, tblname= None):
    if CAT == RESTRICT.CO:
        path_str = 'columns/OBJECT.json'
    elif CAT == RESTRICT.OC:
        path_str = 'columns/OBJECT_CHECK.json'
    elif CAT in [RESTRICT.CM, RESTRICT.PM]:
        path_str = 'columns/{}.json'.format(tblname)
    elif CAT == RESTRICT.FM:
        path_str = 'columns/FX_ALARM.json'
    elif CAT == RESTRICT.DC:
        path_str = 'columns/{}_CHECK.json'.format(tblname)
    return wic.find_config_path().joinpath(path_str)



def find_bak_path(cusid, tech, CAT, tblname= None):
    if CAT == RESTRICT.CO:
        path_str = 'columns_bak/COMMON_OBJECT.json'
    elif CAT == RESTRICT.OC:
        path_str = 'columns/OBJECT_CHECK.json'
    elif CAT in [RESTRICT.CM, RESTRICT.PM]:
        path_str = 'columns_bak/{}.json'.format(tblname)
    elif CAT == RESTRICT.FM:
        path_str = 'columns_bak/FX_ALARM.json'
    elif CAT == RESTRICT.DC:
        path_str = 'columns/{}_CHECK.json'.format(tblname)
    return wic.find_config_path().joinpath(path_str)



def _get_SQL_check_columns(cusid, tech, tblname= None):

    tblcond = 'is null'
    if tblname is None:
        tblcond = '= TABLE_NAME'
    elif type(tblname) is str:
        tblcond = "= '{tblname}'".format(tblname= tblname)
    elif type(tblname) is list:
        tblcond = "in ('{}')".format("', '".join(tblname))
        
    return """
select TABLE_NAME
from CM_DELTA_CHECK_4G
where ENABLED = '1' and TABLE_NAME {tblcond}
           """.format(tblcond= tblcond)



def _get_proc_check_columns():

    """
    ## result: { tblname: set([ field ]) }
    """
    result = set()

    def _f(result, record):
        if record is None:
            return result
        else:
            (TABLE_NAME,) = record
            if TABLE_NAME not in result and TABLE_NAME != '':
                result.add(TABLE_NAME)

    return lambda arg= None: _f(result, arg)



def get_check_columns(tblname, cusid, tech, CAT, mod_name):
    
    if tblname is None:
        SQL = _get_SQL_check_columns(cusid, tech)
    if type(tblname) in set([str, list]):
        SQL = _get_SQL_check_columns(cusid, tech, tblname)

    dbconf = DB1.get_computed_config(cusid, tech, mod_name)[CAT]
    task_name = 'get columns for checking {}'.format(tblname)
    return DB.get_data_set(dbconf, task_name, SQL, _get_proc_check_columns(), mod_name)
