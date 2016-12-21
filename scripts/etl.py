import wic
from wic import RESTRICT
from wic.RESTRICT import (PCOFNSRAW, PCOFNGRAW, IMSCSFRAW, IMSHSSRAW, MADNHRRAW, MADODCRAW, IMSDRARAW, XMLNSSRAW, NOKOBWRAW, NOKOMWRAW, NOKIUMRAW, CM, FM, PM)
from wic import default as Default
from wic import etl as ETL
from wic.etl import check_column as ColCheck
from wic.etl import arbitrary as Patch
from wic.etl import key as SysKey
from wic.etl import param
from wic.etl import ds as DataSource
from wic.etl import db as DB
from wic.etl.param import args_parsing
from wic.etl import co as CO
from wic.etl import cm as CM
from wic.etl import fm as FM
from wic.etl import pm as PM
from wic.etl import dc as DC
from wic.etl import load_data as LoData
from wic.etl import aggregate as Agg
from wic.etl.pm import aggregation as Agg1
from wic.etl import common as Common
from wic import util as Util
from wic.util import logging
from wic.util.logging import logger
import sys, traceback



def do(f, a):

    if f == param.set_current:
        cusid, technology = a
        ETL.set_current(cusid, technology)
        cusid, tech = ETL.get_current()
        logger(__name__).info('current cusid: {}, tech: {}'.format(cusid, tech))
        return

    if f == param.get_current:
        cusid, tech = ETL.get_current()
        logger(__name__).info('current cusid: {}, tech: {}'.format(cusid, tech))
        return
    
    cusid, tech = ETL.get_current()
    if cusid is None or tech is None:
        return

    if f == param.clean_init:
        ETL.clear_working_space(cusid, tech, date= a)
    elif f == param.initialize:
        ETL.initialize_working_space(cusid, tech, date= a)
    elif f == param.show_cfg:
        result = ETL.list_computed_config(cusid, tech, key= a, mod_name= __name__)
        if result is not None:
            print(result)
    elif f == param.show_raw_cfg:
        result = ETL.list_config(cusid, tech, key= a, mod_name= __name__)
        if result is not None:
            print(result)
    elif f == param.show_ds_cfg:
        result = DataSource.list_computed_config(cusid, tech, key= a, mod_name= __name__)
        if result is not None:
            print(result)
    elif f == param.show_raw_ds_cfg:
        result = DataSource.list_config(cusid, tech, key= a, mod_name= __name__)
        if result is not None:
            print(result)
    elif f == param.show_db_cfg:
        result = DB.list_computed_config(cusid, tech, key= a, mod_name= __name__)
        if result is not None:
            print(result)
    elif f == param.show_raw_db_cfg:
        result = DB.list_config(cusid, tech, key= a, mod_name= __name__)
        if result is not None:
            print(result)
    elif f == param.config:
        key, value = a
        ETL.set_config(cusid, tech, key, value, __name__)
    elif f == param.config_ds:
        key, value = a
        DataSource.set_config(cusid, tech, key, value, __name__)
    elif f == param.config_db:
        key, value = a
        DB.set_config(cusid, tech, key, value, __name__)
    ## check-column
    elif f == param.chkcol_cm_load:
        date = a
        ColCheck.chkcol(cusid, tech, date, RESTRICT.CM, latest= False, load= True)

    elif f == param.chkcol_cm_load_latest:
        date = a
        ColCheck.chkcol(cusid, tech, date, RESTRICT.CM, latest= True, load= True)

    elif f == param.chkcol_cm:
        date= a
        ColCheck.chkcol(cusid, tech, date, RESTRICT.CM, latest= False, load= False)
    
    elif f == param.chkcol_cm_latest:
        date= a
        ColCheck.chkcol(cusid, tech, date, RESTRICT.CM, latest= True, load= False)
    #
    elif f == param.chkcol_fm_load:
        date = a
        ColCheck.chkcol(cusid, tech, date, RESTRICT.FM, latest= False, load= True)

    elif f == param.chkcol_fm_load_latest:
        date = a
        ColCheck.chkcol(cusid, tech, date, RESTRICT.FM, latest= True, load= True)

    elif f == param.chkcol_fm:
        date= a
        ColCheck.chkcol(cusid, tech, date, RESTRICT.FM, latest= False, load= False)
    
    elif f == param.chkcol_fm_latest:
        date= a
        ColCheck.chkcol(cusid, tech, date, RESTRICT.FM, latest= True, load= False)
    #
    elif f == param.chkcol_pm_load:
        date = a
        ColCheck.chkcol(cusid, tech, date, RESTRICT.PM, latest= False, load= True)

    elif f == param.chkcol_pm_load_latest:
        date = a
        ColCheck.chkcol(cusid, tech, date, RESTRICT.PM, latest= True, load= True)

    elif f == param.chkcol_pm:
        date= a
        ColCheck.chkcol(cusid, tech, date, RESTRICT.PM, latest= False, load= False)
    
    elif f == param.chkcol_pm_latest:
        date= a
        ColCheck.chkcol(cusid, tech, date, RESTRICT.PM, latest= True, load= False)
    #
    elif f == param.chkcol_case_load:
        case, date = a
        ColCheck.chkcol(cusid, tech, date, case, latest= False, load= True)

    elif f == param.chkcol_case_load_latest:
        case, date = a
        ColCheck.chkcol(cusid, tech, date, case, latest= True, load= True)

    elif f == param.chkcol_case:
        case, date= a
        ColCheck.chkcol(cusid, tech, date, case, latest= False, load= False)
    
    elif f == param.chkcol_case_latest:
        case, date= a
        ColCheck.chkcol(cusid, tech, date, case, latest= True, load= False)
    #
    elif f == param.chkcol_all_load:
        date = a
        ColCheck.chkcol(cusid, tech, date, RESTRICT.FM, latest= False, load= True)
        ColCheck.chkcol(cusid, tech, date, RESTRICT.CM, latest= False, load= True)
        ColCheck.chkcol(cusid, tech, date, RESTRICT.PM, latest= False, load= True)

    elif f == param.chkcol_all_load_latest:
        date = a
        ColCheck.chkcol(cusid, tech, date, RESTRICT.FM, latest= True, load= True)
        ColCheck.chkcol(cusid, tech, date, RESTRICT.CM, latest= True, load= True)
        ColCheck.chkcol(cusid, tech, date, RESTRICT.PM, latest= True, load= True)

    elif f == param.chkcol_all:
        date= a
        ColCheck.chkcol(cusid, tech, date, RESTRICT.FM, latest= False, load= False)
        ColCheck.chkcol(cusid, tech, date, RESTRICT.CM, latest= False, load= False)
        ColCheck.chkcol(cusid, tech, date, RESTRICT.PM, latest= False, load= False)
    
    elif f == param.chkcol_all_latest:
        date= a
        ColCheck.chkcol(cusid, tech, date, RESTRICT.FM, latest= True, load= False)
        ColCheck.chkcol(cusid, tech, date, RESTRICT.CM, latest= True, load= False)
        ColCheck.chkcol(cusid, tech, date, RESTRICT.PM, latest= True, load= False)
    #
    elif f == param.extract_co:
        CO.extract(cusid, tech, date= a)
    elif f == param.extract_cm:
        CM.extract(cusid, tech, date= a)
    elif f == param.extract_fm:
        FM.extract(cusid, tech, date= a)
    elif f == param.extract_pm:
        PM.extract(cusid, tech, date= a)
    elif f == param.extract_pm1:
        term, date = a
        PM.extract(cusid, tech, date= date, CAT= term)
    elif f == param.chk_co:
        try:
            date1, date2 = a
        except:
            pass
    elif f == param.chk_cm:
        date1, date2 = a
        DC.check(cusid, tech, date= date1, date_to= date2)

    elif f == param.load_co:
        CO.load(cusid, tech, date= a)
    ## load-data
    elif f == param.lodata_cm_dry_run:
        date = a
        LoData.lodata(cusid, tech, date, RESTRICT.CM, latest= False, load= False)
        
    elif f == param.lodata_cm_latest_dry_run:
        date = a
        LoData.lodata(cusid, tech, date, RESTRICT.CM, latest= True, load= False)
        
    elif f == param.lodata_cm:
        date = a
        LoData.lodata(cusid, tech, date, RESTRICT.CM, latest= False, load= True)

    elif f == param.lodata_cm_latest:
        date = a
        LoData.lodata(cusid, tech, date, RESTRICT.CM, latest= True, load= True)
    #
    elif f == param.lodata_fm_dry_run:
        date = a
        LoData.lodata(cusid, tech, date, RESTRICT.FM, latest= False, load= False)
        
    elif f == param.lodata_fm_latest_dry_run:
        date = a
        LoData.lodata(cusid, tech, date, RESTRICT.FM, latest= True, load= False)
        
    elif f == param.lodata_fm:
        date = a
        LoData.lodata(cusid, tech, date, RESTRICT.FM, latest= False, load= True)

    elif f == param.lodata_fm_latest:
        date = a
        LoData.lodata(cusid, tech, date, RESTRICT.FM, latest= True, load= True)
    #
    elif f == param.lodata_pm_dry_run:
        date = a
        LoData.lodata(cusid, tech, date, RESTRICT.PM, latest= False, load= False)
        
    elif f == param.lodata_pm_latest_dry_run:
        date = a
        LoData.lodata(cusid, tech, date, RESTRICT.PM, latest= True, load= False)
        
    elif f == param.lodata_pm:
        date = a
        LoData.lodata(cusid, tech, date, RESTRICT.PM, latest= False, load= True)

    elif f == param.lodata_pm_latest:
        date = a
        LoData.lodata(cusid, tech, date, RESTRICT.PM, latest= True, load= True)
    #
    elif f == param.lodata_case_dry_run:
        case, date = a
        LoData.lodata(cusid, tech, date, case, latest= False, load= False)
        
    elif f == param.lodata_case_latest_dry_run:
        case, date = a
        LoData.lodata(cusid, tech, date, case, latest= True, load= False)
        
    elif f == param.lodata_case:
        case, date = a
        LoData.lodata(cusid, tech, date, case, latest= False, load= True)

    elif f == param.lodata_case_latest:
        case, date = a
        LoData.lodata(cusid, tech, date, case, latest= True, load= True)
    #
    elif f == param.lodata_all_dry_run:
        date = a
        LoData.lodata(cusid, tech, date, RESTRICT.CM, latest= False, load= False)
        LoData.lodata(cusid, tech, date, RESTRICT.FM, latest= False, load= False)
        LoData.lodata(cusid, tech, date, RESTRICT.PM, latest= False, load= False)
        
    elif f == param.lodata_all_latest_dry_run:
        date = a
        LoData.lodata(cusid, tech, date, RESTRICT.CM, latest= False, load= False)
        LoData.lodata(cusid, tech, date, RESTRICT.FM, latest= False, load= False)
        LoData.lodata(cusid, tech, date, RESTRICT.PM, latest= True, load= False)
        
    elif f == param.lodata_all:
        date = a
        LoData.lodata(cusid, tech, date, RESTRICT.CM, latest= False, load= False)
        LoData.lodata(cusid, tech, date, RESTRICT.FM, latest= False, load= False)
        LoData.lodata(cusid, tech, date, RESTRICT.PM, latest= False, load= True)

    elif f == param.lodata_all_latest:
        date = a
        LoData.lodata(cusid, tech, date, RESTRICT.CM, latest= False, load= False)
        LoData.lodata(cusid, tech, date, RESTRICT.FM, latest= False, load= False)
        LoData.lodata(cusid, tech, date, RESTRICT.PM, latest= True, load= True)
    ## aggregate
    elif f == param.aggregate_pm_dry_run:
        date = a
        Agg.aggregate(cusid, tech, date, RESTRICT.PM, latest= False, load= False)
        
    elif f == param.aggregate_pm_latest_dry_run:
        date = a
        Agg.aggregate(cusid, tech, date, RESTRICT.PM, latest= True, load= False)
        
    elif f == param.aggregate_pm:
        date = a
        Agg.aggregate(cusid, tech, date, RESTRICT.PM, latest= False, load= True)

    elif f == param.aggregate_pm_latest:
        date = a
        Agg.aggregate(cusid, tech, date, RESTRICT.PM, latest= True, load= True)
    #
    elif f == param.aggregate_case_dry_run:
        case, date = a
        Agg.aggregate(cusid, tech, date, case, latest= False, load= False)
        
    elif f == param.aggregate_case_latest_dry_run:
        case, date = a
        Agg.aggregate(cusid, tech, date, case, latest= True, load= False)
        
    elif f == param.aggregate_case:
        case, date = a
        Agg.aggregate(cusid, tech, date, case, latest= False, load= True)

    elif f == param.aggregate_case_latest:
        case, date = a
        Agg.aggregate(cusid, tech, date, case, latest= True, load= True)
    ## patch
    elif f == param.patch:
        case, date = a
        Patch.arbitrary(cusid, tech, date, case, latest= False, load= True)
    elif f == param.patch_dry_run:
        case, date = a
        Patch.arbitrary(cusid, tech, date, case, latest= False, load= False)
    elif f == param.patch_latest:
        case, date = a
        Patch.arbitrary(cusid, tech, date, case, latest= True, load= True)
    elif f == param.patch_latest_dry_run:
        case, date = a
        Patch.arbitrary(cusid, tech, date, case, latest= True, load= False)
    #
    elif f == param.load_cm:
        date = a
        DC.load(cusid, tech, date)
    
    elif f == param.load_database:
        Common.load_database(date= a)

    elif f == param.gen_scripts:
        Agg1.gen_scripts(cusid, tech, date= a)

    elif f == param.gen_specific_scripts:
        c, d = a
        Agg1.gen_specific_scripts(cusid, tech, date= d, case= c.upper())
        
    elif f == param.exe_sql_sc:
        path = a
    elif f == param.count_src:
        count = Util.count_src(path= '.' if a is None else a)
        logger(__name__).info('all {:,} line{}'.format(count, '' if count < 2 else 's'))




if __name__ == '__main__':

    logging.config(Default.LOG_FMT)

    #logger(__name__).debug(sys.argv)
    #logger(__name__).debug(args_parsing.parse_args())
    x = param.meet(args_parsing.parse_args())
    f, a = None, None
    try:
        f, a = x
    except Exception as e:
        print(param.param_spec)

    try:
        do(f, a)
        sys.exit(Default.OK)
    except Exception as e:
        logging.switch_to_normal(__name__)
        logger(__name__).error('do: {}: {}'.format(type(e), e))
        traceback.print_exc()
