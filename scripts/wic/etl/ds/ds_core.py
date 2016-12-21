import customer
import wic
from wic.RESTRICT import (DATASOURCE, PROTOCOL, HOST, PORT, USER, PSWD, PATH, CTGR,
                          ZIP_FLT, CSV_FLT, DB, CO, OC, CM, DC, FM, PM, FTP,
                          DSCONF_FILE_NAME)
from wic.etl import key as SysKey
from wic.util import file as File
from wic.util import json as JSON
from wic.util.logging import logger
import re, json, yaml


#def _get_default_config():
#    return { PROTOCOL: FTP,
#             HOST: None,
#             PORT: None,
#             USER: None,
#             PSWD: None,
#             PATH: None,
#             CTGR: '{date:%Y%m}/{date:%Y%m%d}' }


#def _find_config_path():
#    base = wic.find_config_path()
#    return base.joinpath(DSCONF_FILE_NAME)


def create_config_file(cusid, tech, mod_name):
    path = wic.find_DS_config_file_path()
    #logger(__name__).debug(path)
    if not path.exists():
        File.dump_JSON(path, customer.get_default_DS_config(cusid, tech), mod_name)

def get_config(cusid, tech, mod_name):
    path = wic.find_DS_config_file_path()
    try:
        return File.load_JSON(path, mod_name)
    except: # if system is not initialized
        return customer.get_defaultDS_config_all(cusid, tech)

def get_computed_config(cusid, tech, mod_name):
    json_data = customer.get_default_DS_config_all(cusid, tech)
    json_data1 = get_config(cusid, tech, __name__)
    for k in json_data1:
        if k in json_data1 and json_data1[k] is not None:
            json_data[k] = json_data1[k]
    return json_data


def set_config(cusid, tech, key, value, mod_name):
    path = wic.find_DS_config_file_path()
    json_data = File.load_JSON(path, mod_name)
    if key is not None and key.upper() in json_data:
        key1 = key.upper()
        if key1 == 'PORT':
            try:
                json_data[key1] = int(value)
                File.dump_JSON(path, json_data, mod_name)
            except Exception as e:
                logger(__name__).warning('bad value "{}": {} {}'.format(value, type(e), e))
        else:
            json_data[key1] = value
            File.dump_JSON(path, json_data, mod_name)


            
def list_config(cusid, tech, key, mod_name):
    path = wic.find_DS_config_file_path()
    logger(__name__).info('show "{}"'.format(path))
    json_data = File.load_JSON(path, mod_name)
    return JSON.to_yaml(json_data, key)



def list_computed_config(cusid, tech, key, mod_name):
    json_data = get_computed_config(cusid, tech, mod_name)
    json_data1 = dict()
    for k in json_data:
        if k.endswith('_FLT'):
            continue
        json_data1[k] = json_data[k]
    return JSON.to_yaml(json_data1, key)
