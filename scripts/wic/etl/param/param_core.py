from wic import util as Util
from wic.util.logging import logger
import datetime, re


##
## Get Help Page
##

def _get_build_year():
    t = datetime.datetime.today()
    if t.year == 2016:
        return t.strftime('%Y')
    elif t.year > 2016:
        return '2016 ~ ' + t.strftime('%Y')

param_spec = "WCEP: WiC ETL Progress (c) {} WiC Co. Ltd.".format(_get_build_year()) +\
"""
usage:
    etl COMMAND [--OPTION [...]]
    etl COMMAND [--OPTION [...]] KEY
    etl COMMAND [--OPTION [...]] KEY VALUE

examples:
    etl current    <cusid>       <tech>
    etl current
    etl init
    etl init [-c]  <yyymmdd>
    etl list       {conf|ds|db}  [key|key.key]
    etl configure  {conf|ds|db}  {{key|key.key}} value
    etl chk-column <yyyymmdd>
    etl chk-column {cm|fm|pm}    <yyyymmdd> [-o|--old] [-l|--load]
    etl extract    {co|cm|fm|pm} <yyyymmdd>
    etl check      {co|cm}       <later_date>    <earlier_date>
    etl update     <yyyymmdd>
    etl load-data  {cm|fm|pm}    <yyyymmdd> [-o|--old] [-l|--load]
    etl aggregate  pm            <yyyymmdd> [-o|--old] [-l|--load]
    etl gen-script pm            <yyyymmdd>
    etl execute    <path>
    etl patch      {cm|fm|pm}    <yyyymmdd> [-o|--old] [-l|--load]

commands:
    current  <cusid> <tech>   switch context for specific customer
    current                   show current customer
    init                      build system workspace
    init      <yyyymmdd>      build daily workspace
    intt -c   <yyyymmdd>      reset daily workspace
    list      conf            list or filter system setting values
    list      ds              list or filter datasource setting values
    list      db              list or filter database setting values
    configure conf            set system setting
    configure ds              set datasource setting
    configure db              set database setting     
    chk-column                generate SQL suggestion for columns
    chk-column --load         generate SQL suggestion for columns and send to database
    extract   {CO|CM|FM|PM}   do main task
    check     CO              perform Object Check
    check     CM              perform Delta Check
    update    <yyyymmdd>      upload data to database
    load      <yyyymmdd>      upload data to database
    load -l   <yyyymmdd>      copy data of <yyyymmdd> on database to the latest table
    execute   <path>          execute some SQL script
    patch     {cm|fm|pm}      execute arbitrary procedures

options:
    --clean, -c   with deleting workspace

alias:
    list    ls
    remote  rm
    extract take, extract
    check   chk
    load    up, update, upload
    execute exec
    conf    cfg, config, configuration
    ds      datasource
    db      database
    co      common_object
    cm      configuration_management
    fm      alarm, fx_alarm
    pm      performance, performance_measurement
"""

def _analyze_args(args):
    cmds = list()
    opts = set()
    for x in args:
        if x.startswith('--'):
            opts.add(x.lower())
        else:
            cmds.append(x)
    return (cmds[1].lower() if len(cmds) > 1 else None,
            cmds[2].lower() if len(cmds) > 2 else None,
            cmds[3].lower() if len(cmds) > 3 else None,
            cmds[4] if len(cmds) > 4 else None,
            opts)

def set_current(spec):
    cusid, technology = spec

def get_current():
    pass

def clean_init(date):
    pass

def initialize(date):
    pass

def count_src(key):
    pass

def show_cfg(key):
    if key is None:
        pass
    else:
        pass

def show_raw_cfg(key):
    if key is None:
        pass
    else:
        pass
    
def show_ds_cfg(key):
    if key is None:
        pass
    else:
        pass

def show_raw_ds_cfg(key):
    if key is None:
        pass
    else:
        pass

def show_db_cfg(keyOr2Keys):
    if keyOr2Keys is None:
        pass
    else:
        pass

def show_raw_db_cfg(keyOr2Keys):
    if keyOr2Keys is None:
        pass
    else:
        pass
    
def config(kv):
    pass

def config_ds(kv):
    pass

def config_db(dv):
    pass

## check-column --old
def chkcol_cm(d): pass
def chkcol_fm(d): pass
def chkcol_pm(d): pass
def chkcol_case(a): case, data = a
def chkcol_all(d): pass

## check-column (latest)
def chkcol_cm_latest(d): pass
def chkcol_fm_latest(d): pass
def chkcol_pm_latest(d): pass
def chkcol_case_latest(a): case, data = a
def chkcol_all_latest(d): pass

## check-column --load --old
def chkcol_cm_load(d): pass
def chkcol_fm_load(d): pass
def chkcol_pm_load(d): pass
def chkcol_all_load(d): pass
def chkcol_case_load(a): case, date = a

## check-column --load (latest)
def chkcol_cm_load_latest(d): pass
def chkcol_fm_load_latest(d): pass
def chkcol_pm_load_latest(d): pass
def chkcol_all_load_latest(d): pass
def chkcol_case_load_latest(a): case, date = a

## load-data --old
def lodata_cm_dry_run(d): pass
def lodata_fm_dry_run(d): pass
def lodata_pm_dry_run(d): pass
def lodata_case_dry_run(a): case, date = a
def lodata_all_dry_run(d): pass

## load-data --old --load
def lodata_cm(d): pass
def lodata_fm(d): pass
def lodata_pm(d): pass
def lodata_case(a): case, date = a
def lodata_all(d): pass

## load-data
def lodata_cm_latest_dry_run(d): pass
def lodata_fm_latest_dry_run(d): pass
def lodata_pm_latest_dry_run(d): pass
def lodata_case_latest_dry_run(a): case, date = a
def lodata_all_latest_dry_run(d): pass

## load-data --load
def lodata_cm_latest(d): pass
def lodata_fm_latest(d): pass
def lodata_pm_latest(d): pass
def lodata_case_latest(a): case, date = a
def lodata_all_latest(d): pass

## aggregate --old
def aggregate_pm_dry_run(d): pass
def aggregate_case_dry_run(a): case, date = a

## aggregate --old --load
def aggregate_pm(d): pass
def aggregate_case(a): case, date = a

## aggregate
def aggregate_pm_latest_dry_run(d): pass
def aggregate_case_latest_dry_run(a): case, date = a

## aggregate --load
def aggregate_pm_latest(d): pass
def aggregate_case_latest(a): case, date = a

## patch
def patch(a): case, date = a
def patch_latest(a): case, date = a
def patch_dry_run(a): case, date = a
def patch_latest_dry_run(a): case, date = a

def extract_co(date):
    pass

def extract_cm(date):
    pass

def extract_fm(date):
    pass

def extract_pm(date):
    pass

def extract_pm1(a):
    term, date = a

def chk_co(two_dates):
    pass

def chk_cm(two_dates):
    pass

def load_co(date):
    pass

def load_cm(date):
    pass

def load_latest_cm(date):
    pass

def load_fm(date):
    pass

def load_latest_fm(date):
    pass

def load_pm(date):
    pass

def load_latest_pm(date):
    pass

def load_database(key):
    if type(key) is datetime.date:
        pass

def gen_scripts(key):
    if type(key) is datetime.date:
        pass

def gen_specific_scripts(a):
    pm_case, d= a
    if type(d) is datetime.date:
        pass
    
def exe_sql_sc(path):
    pass

##
## Strategy
##
## When you find some value which is not dict() deep in the nested `cmd_all', it's a valid command,
## else not.
##
def meet(args):
    if type(args) is list:
        
        cmd, target, key, value, opts = _analyze_args(args)
        logger(__name__).debug('command: {} {} {} {}'.format(cmd, target, key, value))
        logger(__name__).debug('options: {}'.format(str(opts)))

        current = set(['cur', 'current'])
        init = set(['init'])
        ls = set(['ls', 'list'])
        conf = set(['conf', 'cfg', 'config', 'configuration'])
        ds = set(['ds', 'datasource'])
        db = set(['db', 'database'])
        configure = set(['configure', 'config', 'set'])
        extract = set(['take', 'extract'])
        chkcol = set(['chk-column', 'chk-columns', 'check-column', 'check-columns', 'chk-col', 'chk-cols', 'check-col', 'check-cols'])
        chk = set(['chk', 'check'])
        load = set(['load', 'up', 'upload', 'update'])
        load_data = set(['load-data', 'ld'])
        aggregate = set(['aggregate', 'aggregation', 'agg'])
        script = set(['script', 'scripts', 'generate-script', 'generate-scripts', 'gen-script', 'gen-scripts'])
        ex = set(['exec', 'execute'])
        patch = set(['patch', 'arbitrary'])
        co = set(['co', 'CO', 'common_object'])
        cm = set(['cm', 'CM', 'configuration_management'])
        fm = set(['fm', 'FM', 'alarm', 'fx_alarm'])
        pm = set(['pm', 'PM', 'performance', 'performance_measurement'])
        pm1 = set(['pcofnsraw', 'pcofngraw', 'imscsfraw', 'imshssraw', 'madnhrraw', 'madodcraw', 'imsdraraw', 'xmlnssraw', 'nokobwraw', 'nokomwraw', 'nokiumraw'])
        oc = set(['oc', 'object_check'])
        dc = set(['dc', 'delta_check'])
        col = set(['col', 'columns'])
        opt_old = set(['--old', '-o']) & opts != set()
        opt_load = set(['--load', '-l']) & opts != set()
        is_date = re.compile(r'^\d\d\d\d\d\d\d\d$')
        if cmd in current:
            if target is None:
                return get_current, None
            elif target is not None and key is not None:
                return set_current, (target.upper(), key.lower())
        elif cmd in init:
            if target is None:
                return initialize, None
            else:
                d = Util.conv_date(target)
                if type(d) is datetime.date:
                    if '--clean' in opts or '-c' in opts:
                        return clean_init, d
                    else:
                        return initialize, d
        elif cmd in ls:
            if target == 'src':
                return count_src, key
            elif target in conf and value is None:
                if '--raw' in opts:
                    return show_raw_cfg, key
                else:
                    return show_cfg, key
            elif target in ds and value is None:
                if '--raw' in opts:
                    return show_raw_ds_cfg, key
                else:
                    return show_ds_cfg, key
            elif target in db and value is None:
                if '--raw' in opts:
                    return show_raw_db_cfg, key
                else:
                    return show_db_cfg, key
        elif cmd in configure:
            if key is not None and value is not None:
                if target in conf:
                    return config, (key, value)
                elif target in ds:
                    return config_ds, (key, value)
                elif target in db:
                    return config_db, (key, value)

        elif cmd in chkcol:
            d = Util.conv_date(key)
            
            if   target in cm and     opt_old and     opt_load: return chkcol_cm_load,        d
            elif target in cm and not opt_old and     opt_load: return chkcol_cm_load_latest, d
            elif target in cm and     opt_old and not opt_load: return chkcol_cm,             d
            elif target in cm and not opt_old and not opt_load: return chkcol_cm_latest,      d

            elif target in fm and     opt_old and     opt_load: return chkcol_fm_load,        d
            elif target in fm and not opt_old and     opt_load: return chkcol_fm_load_latest, d
            elif target in fm and     opt_old and not opt_load: return chkcol_fm,             d
            elif target in fm and not opt_old and not opt_load: return chkcol_fm_latest,      d

            elif target in pm and     opt_old and     opt_load: return chkcol_pm_load,        d
            elif target in pm and not opt_old and     opt_load: return chkcol_pm_load_latest, d
            elif target in pm and     opt_old and not opt_load: return chkcol_pm,             d
            elif target in pm and not opt_old and not opt_load: return chkcol_pm_latest,      d

            elif target in pm1 and     opt_old and     opt_load: return chkcol_case_load,        (target.upper(), d)
            elif target in pm1 and not opt_old and     opt_load: return chkcol_case_load_latest, (target.upper(), d)
            elif target in pm1 and     opt_old and not opt_load: return chkcol_case,             (target.upper(), d)
            elif target in pm1 and not opt_old and not opt_load: return chkcol_case_latest,      (target.upper(), d)

            else:
                d = Util.conv_date(target)
            
                if   d is not None and     opt_old and     opt_load: return chkcol_all_load,        d
                elif d is not None and not opt_old and     opt_load: return chkcol_all_load_latest, d
                elif d is not None and     opt_old and not opt_load: return chkcol_all,             d
                elif d is not None and not opt_old and not opt_load: return chkcol_all_latest,      d

        elif cmd in load_data:
            d = Util.conv_date(key)
            
            if   target in cm and     opt_old and     opt_load: return lodata_cm,                d
            elif target in cm and not opt_old and     opt_load: return lodata_cm_latest,         d
            elif target in cm and     opt_old and not opt_load: return lodata_cm_dry_run,        d
            elif target in cm and not opt_old and not opt_load: return lodata_cm_latest_dry_run, d

            elif target in fm and     opt_old and     opt_load: return lodata_fm,                d
            elif target in fm and not opt_old and     opt_load: return lodata_fm_latest,         d
            elif target in fm and     opt_old and not opt_load: return lodata_fm_dry_run,        d
            elif target in fm and not opt_old and not opt_load: return lodata_fm_latest_dry_run, d

            elif target in pm and     opt_old and     opt_load: return lodata_pm,                d
            elif target in pm and not opt_old and     opt_load: return lodata_pm_latest,         d
            elif target in pm and     opt_old and not opt_load: return lodata_pm_dry_run,        d
            elif target in pm and not opt_old and not opt_load: return lodata_pm_latest_dry_run, d

            elif target in pm1 and     opt_old and     opt_load: return lodata_case,                (target.upper(), d)
            elif target in pm1 and not opt_old and     opt_load: return lodata_case_latest,         (target.upper(), d)
            elif target in pm1 and     opt_old and not opt_load: return lodata_case_dry_run,        (target.upper(), d)
            elif target in pm1 and not opt_old and not opt_load: return lodata_case_latest_dry_run, (target.upper(), d)

            else:
                d = Util.conv_date(target)
            
                if   d is not None and     opt_old and     opt_load: return lodata_all,                d
                elif d is not None and not opt_old and     opt_load: return lodata_all_latest,         d
                elif d is not None and     opt_old and not opt_load: return lodata_all_dry_run,        d
                elif d is not None and not opt_old and not opt_load: return lodata_all_latest_dry_run, d

        elif cmd in aggregate:
            d = Util.conv_date(key)
            
            if   target in pm and     opt_old and     opt_load: return aggregate_pm,                d
            elif target in pm and not opt_old and     opt_load: return aggregate_pm_latest,         d
            elif target in pm and     opt_old and not opt_load: return aggregate_pm_dry_run,        d
            elif target in pm and not opt_old and not opt_load: return aggregate_pm_latest_dry_run, d

            elif target in pm1 and     opt_old and     opt_load: return aggregate_case,                (target.upper(), d)
            elif target in pm1 and not opt_old and     opt_load: return aggregate_case_latest,         (target.upper(), d)
            elif target in pm1 and     opt_old and not opt_load: return aggregate_case_dry_run,        (target.upper(), d)
            elif target in pm1 and not opt_old and not opt_load: return aggregate_case_latest_dry_run, (target.upper(), d)

        elif cmd in patch and (target in cm or target in fm or target in pm or target in pm1):
            d = Util.conv_date(key)
            
            if       opt_old and     opt_load: return patch,                (target.upper(), d)
            elif not opt_old and     opt_load: return patch_latest,         (target.upper(), d)
            elif     opt_old and not opt_load: return patch_dry_run,        (target.upper(), d)
            elif not opt_old and not opt_load: return patch_latest_dry_run, (target.upper(), d)

        elif cmd in extract:
            if target in co:
                key = Util.conv_date(key)
                return extract_co, key
            elif target in cm:
                key = Util.conv_date(key)
                return extract_cm, key
            elif target in fm:
                key = Util.conv_date(key)
                return extract_fm, key
            elif target in pm:
                key = Util.conv_date(key)
                return extract_pm, key
            elif target in pm1:
                key = Util.conv_date(key)
                return extract_pm1, (target.upper(), key)
        elif cmd in chk:
            if target in co or target in cm:
                key = Util.conv_date(key)
                value = Util.conv_date(value)
                if type(key) is datetime.date and type(value) is datetime.date:
                    if target in co:
                        return chk_co, (key, value)
                    elif target in cm:
                        return chk_cm, (key, value)

        elif cmd in load:
            if target in cm:
                key = Util.conv_date(key)
                if type(key) is datetime.date:
                    return load_cm, key
                    
        elif cmd in script:

            key = Util.conv_date(key)
            if target in pm and type(key) is datetime.date:

                return gen_scripts, key

            elif target in pm1 and type(key) is datetime.date:

                return gen_specific_scripts, (target, key)
            
        elif cmd in ex:
            if target is not None:
                return exe_sql_sc, target
        
