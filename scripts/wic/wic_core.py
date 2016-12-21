from . import RESTRICT
from . import default as Default
from wic import util as Util
from wic.util.logging import logger
import sys, pathlib



def find_config_path():

    confpath = pathlib.Path('{base}/{folder}/current'.format(
        base= Default.CONFIG_PATH,
        folder= Default.CONFIG_FOLDER)
    )
    if confpath.exists():
        with open(str(confpath), 'r') as fo:
            i = -1
            for i, ln in enumerate(fo):
                break
        if i == 0:
            return pathlib.Path(ln.rstrip())



def find_config_file_path():
    confpath = find_config_path()
    if confpath is not None:
        return confpath.joinpath(RESTRICT.CONF_FILE_NAME)



def find_DS_config_file_path():
    confpath = find_config_path()
    if confpath is not None:
        return confpath.joinpath(RESTRICT.DSCONF_FILE_NAME)



def find_DB_config_file_path():
    confpath = find_config_path()
    if confpath is not None:
        return confpath.joinpath(RESTRICT.DBCONF_FILE_NAME)



def find_data_path(cusid= None, tg= None):

    if cusid is None and tg is None:
        try:
            return pathlib.Path(Default.DATA_PATH).resolve()
        except Exception as e:
            logger(__name__).debug('{}: {}'.format(type(e), e))
            logger(__name__).critical('bad DATA_PATH; check configuration')
            sys.exit(Default.INTERRUPT)

    elif tg is None:
        return find_data_path().joinpath(cusid)

    else:
        return find_data_path().joinpath(cusid).joinpath(tg)

