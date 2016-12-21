from wic import RESTRICT
from wic.util.logging import logger
import pkgutil



def get_default_config(cusid, tech):
    return { RESTRICT.CONFIG_PATH: None,
             RESTRICT.CONFIG_FOLDER: None,
             RESTRICT.DATA_PATH: None }



def get_default_DB_config(cusid, tech):
    tech = __import__('customer.{}.{}'.format(cusid, tech.lower()), globals(), locals(), ['system_key'], 0)
    return tech.system_key[RESTRICT.DATABASES]



def get_default_DS_config_all(cusid, tech):
    tech = __import__('customer.{}.{}'.format(cusid, tech.lower()), globals(), locals(), ['system_key'], 0)
    return tech.system_key[RESTRICT.DATASOURCE]



def get_default_DS_config(cusid, tech):
    result = get_default_DS_config_all(cusid, tech)
    result.pop(RESTRICT.ZIP_FLT)
    result.pop(RESTRICT.CSV_FLT)
    return result
