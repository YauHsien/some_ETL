import customer
import wic
from wic import RESTRICT
from wic import default as Default
from wic.etl import key as SysKey
from wic.etl import ds as DataSource
from wic.etl import db as DB
from wic import util as Util
from wic.util.logging import logger
from wic.util import folder as Folder
from wic.util import file as File
from wic.util import ftp as FTP
from wic.util import json as JSON
from wic.util import is_function, get_arity
import inspect, sys, pathlib, re, datetime, yaml, imp, traceback



#def _get_default_config():
#    return { 'CONFIG_PATH': None,
#             'CONFIG_FOLDER': None,
#             'DATA_PATH': None }



#def _find_config_path(cusid, tech):
#    base = wic.find_config_path()
#    return base.joinpath(RESTRICT.CONF_FILE_NAME)



def create_config_file(cusid, tech, mod_name):
    path = wic.find_config_file_path()
    #logger(__name__).debug(path)
    if not path.exists():
        File.dump_JSON(path, customer.get_default_config(cusid, tech), mod_name)



def get_config(cusid, tech, mod_name):
    path = wic.find_config_file_path()
    #logger(__name__).debug(path)
    try:
        return File.load_JSON(path, mod_name)
    except:
        return { RESTRICT.CONFIG_PATH: pathlib.Path(Default.CONFIG_PATH),
                 RESTRICT.CONFIG_FOLDER: Default.CONFIG_FOLDER,
                 RESTRICT.DATA_PATH: pathlib.Path(Default.DATA_PATH) }



def get_computed_config(cusid, tech, mod_name):
    json_data = get_config(cusid, tech, mod_name)
    if type(json_data) is dict:
        json_data1 = dict()
        for k in json_data:
            if k in dir(Default):
                if json_data[k] is None:
                    if k.endswith('_PATH'):
                        json_data1[k] = pathlib.Path(inspect.getattr_static(Default, k))
                    else:
                        json_data1[k] = inspect.getattr_static(Default, k)
                else:
                    if k.endswith('_PATH'):
                        json_data1[k] = pathlib.Path(json_data[k])
                    else:
                        json_data1[k] = json_data[k]
        #logger(__name__).debug(json_data1)
        return json_data1



def set_config(cusid, tech, key, value, mod_name):
    if key is not None:
        path = wic.find_config_file_path()
        json_data = File.load_JSON(path, mod_name)
        key1 = key.upper()
        if key1 in json_data:
            if key1.endswith('_PATH'):
                logger(__name__).info('varifying path "{}"...'.format(value))
                try:
                    pathlib.Path(value).resolve()
                    json_data[key1] = value
                except Exception as e:
                    logger(__name__).warning('bad path: {} {}'.format(type(e), e))
            else:
                json_data[key1] = value
            File.dump_JSON(path, json_data, mod_name)
        


def list_config(cusid, tech, key, mod_name):
    path = wic.find_config_file_path()
    logger(__name__).info('show "{}"'.format(path))
    json_data = File.load_JSON(path, mod_name)
    for k in json_data:
        if File.is_path(json_data[k]):
            if not File.exists(json_data[k]):
                logger(__name__).warning('bad path: "{}"'.format(str(json_data[k])))
    return JSON.to_yaml(json_data, key)



def list_computed_config(cusid, tech, key, mod_name):
    json_data = get_computed_config(cusid, tech, mod_name)
    for k in json_data:
        if File.is_path(json_data[k]):
            if not File.exists(json_data[k]):
                logger(__name__).warning('bad path: "{}"'.format(str(json_data[k])))
            json_data[k] = '{} {}'.format(type(json_data[k]),
                                               json_data[k])
    return JSON.to_yaml(json_data, key)



def _find_current_candidate(confpath):

    logger(__name__).warning("you may want to use 'etl current <cusid> <tech>' to switch current working space")
    lines = list()
    for fdrpath in confpath.glob('*/*'):
        if fdrpath.is_dir():
            lines.append(fdrpath)
    if lines != list():
        logger(__name__).warning('you have:')
        for fdrpath in lines:
            logger(__name__).warning('cusid= {}, tech= {}'.format(fdrpath.parent.name, fdrpath.name))




def set_current(cusid, technology):
    confpath = Default.CONFIG_PATH.joinpath('{}/current'.format(Default.CONFIG_FOLDER))
    
    if Util.has_module('customer.{cusid}.{tech}'.format(cusid= cusid, tech= technology)):
        pathconf = str(confpath.parent.joinpath(cusid).joinpath(technology.upper()))
        with open(str(confpath), 'w') as fo:
            fo.write(pathconf)
        logger(__name__).info('written: "{}"'.format(confpath))
        logger(__name__).info('using config in "{}"'.format(pathconf))
        
    else:
        if confpath.exists():
            with open(str(confpath), 'r') as fo:
                for ln in iter(fo):
                    logger(__name__).info('using config in "{}"'.format(ln))
                
        else:
            logger(__name__).info('not found: "{}"'.format(str(confpath)))

    _find_current_candidate(confpath.parent)



def get_current():
    confpath = Default.CONFIG_PATH.joinpath('{}/current'.format(Default.CONFIG_FOLDER))
    cusid, tech = None, None

    line = None
    if confpath.exists():
        with open(str(confpath), 'r') as fo:
            for ln in iter(fo):
                line = pathlib.Path(ln.rstrip())
                break

    if line is None:
        logger(__name__).info('not found: "{}"'.format(str(confpath)))
    elif line.parent.parent == confpath.parent:
        cusid, tech = line.parent.name, line.name
    else:
        logger(__name__).info('bad current: "{}"'.format(str(line)))
        
    _find_current_candidate(confpath.parent)
        
    return cusid, tech
            


def clear_working_space(cusid, tech, date):
    if type(date) is datetime.date:
        base = wic.find_config_path().joinpath('{date:%Y%m%d}'.format(date= date))
        logger(__name__).info('cleaning "{}"...'.format(str(base)))
        if base.exists():
            Folder.remove(base, __name__)
        initialize_working_space(cusid, tech, date)



def _find_data_path(cusid, tech):
    #logger(__name__).debug(get_computed_config(cusid, tech, __name__))
    dppath = get_computed_config(cusid, tech, __name__)[RESTRICT.DATA_PATH]
    if dppath.exists():
        return dppath.joinpath(cusid).joinpath(tech)
    else:
        logger(__name__).warning('bad config: DATA_PATH "{}"'.format(str(dppath)))
        


def initialize_working_space(cusid, tech, date= None):

    base = wic.find_config_path()

    if date is None:
        logger(__name__).info('initializing "{}".....'.format(base))
        for folder in ['columns', 'columns_bak']:
            Folder.create(base.joinpath(folder), __name__)

        create_config_file(cusid, tech, __name__)
        DataSource.create_config_file(cusid, tech, __name__)
        DB.create_config_file(cusid, tech, __name__)

        dpbase1 = _find_data_path(cusid, tech)
        if dpbase1 is not None:
            for folder in ['cache']:
                Folder.create(dpbase1.joinpath(folder), __name__)
                for p, c in [('COMMON_OBJECT.json',
                              lambda f: File.dump_JSON(f, dict(), __name__))]:
                    f = dpbase1.joinpath(p)
                    if f.exists():
                        logger(__name__).info('found: "{}"'.format(str(f)))
                    else:
                        c(f)

    elif type(date) is datetime.date:
        ymd = '{:%Y%m%d}'.format(date)
        base = base.joinpath(ymd)
        fcheck = base.joinpath('files.txt')
        logger(__name__).info('initailizing "{}"...'.format(fcheck))
        
        if not fcheck.exists():
            Folder.create(base, __name__)
            filelist = get_data_source_list(cusid, tech, date)
            if filelist == []:
                sys.exit()
            with open(str(fcheck), 'w') as fo:
                for _, ln in enumerate(filelist):
                    fo.write('{}\n'.format(ln))
                fo.close()

            flz = DataSource.get_computed_config(cusid, tech, __name__)[RESTRICT.ZIP_FLT]
            LRCs = set()
            for _, p in enumerate(filelist):
                fn = pathlib.Path(p).name
                for _, r in enumerate(flz):
                    m = flz[r].match(fn)
                    if type(m) is type(re.match('', '')):
                        try:
                            LRCs.add((r, m.group(3) if r is 'PM' else m.group(1)))
                        except Exception as e:
                            logger(__name__).debug('{}: {}'.format(m, e))
                        break
            for CAT, LRC in LRCs:
                Folder.create(base.joinpath(CAT).joinpath(LRC), __name__)

            Folder.create(base.joinpath('history'), __name__)

        dpbase = _find_data_path(cusid, tech)
        ymdbase = dpbase.joinpath(ymd)
        for _, fdrpath in enumerate([ 'tmp', 'columns/check', 'cache/OC',
                                      RESTRICT.CO, RESTRICT.CM,
                                      RESTRICT.OC, RESTRICT.DC,
                                      RESTRICT.FM, RESTRICT.PM  ]):
            Folder.create(ymdbase.joinpath(fdrpath), __name__)



def get_data_source_list(cusid, tech, date):
    if type(date) is datetime.date:
        ds = DataSource.get_computed_config(cusid, tech, __name__)
        path = pathlib.Path('{path}/{category}'.format(path= ds[RESTRICT.PATH], category= ds[RESTRICT.CTGR].format(date= date)))
        nlst = FTP.nlst(ds[RESTRICT.PROTOCOL], ds[RESTRICT.HOST], ds[RESTRICT.PORT], ds[RESTRICT.USER], ds[RESTRICT.PSWD], path.as_posix(), __name__)
        return nlst
