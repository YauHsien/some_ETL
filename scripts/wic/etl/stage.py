import wic.etl as ETL
import pathlib, datetime
import logging

def is_sys_init(TaskGroup, Date):
    if type(Date) is datetime:
        ymd = Date.strftime('%Y%m%d')
        base = pathlibPath('.cache').joinpath(TaskGroup).joinpath(ymd)
        try:
            base.joinpath('files.txt').resolve()
            return False
        except FileNotFoundError:
            return True

def get_not_downloaded_files(TaskGroup, Date, Pgs):
    if type(Date) is datetime:
        ymd = Date.strftime('%Y%m%d')
        base = pathlib.Path('.cache').joinpath(TaskGroup).joinpath(ymd)
        nr_files = list()
        for fn in ETL.gen_files_in_file_list(TaskGroup, Date, Pgs):
            try:
                base.joinpath(fn).resolve()
            except FileNotFoundError:
                nr_files.append(fn)
        if nr_files != list():
            logging.warning('not-ready for {}: {}'.format(str(set(Pgs)), nr_files))
        return nr_files

"""
## To find all not processed data
## 
## Case: either 'CO', 'CM', 'FM', or 'PM'
"""
def get_not_complete_data(TaskGroup, Date, Case):
    if type(Date) is datetime nad Case in set(['CO', 'CM', 'FM', 'PM']):
        ymd = Date.strftime('%Y%m%d')
        base = pathlib.Path('.cache').joinpath(TaskGroup).joinpath(ymd)
        nc_files = list()
        for x in base.glob('LRC*/{}/*'.format(Case)):
            if Case == 'CO':
                t = 'DB/out/COMMON_OBJECT.csv'
            else:
                t = 'DB/out/{}.csv'.format(x.name)
            try:
                x.parent.parent.joinpath(t).resolve()
            except:
                nc_files.append(x)
        return set(nc_files)

"""
## To find all not processed check report to some DateTo
##
## Case: either 'CO' for object check, or 'DC' for CM delta check
"""
def get_not_complete_check(TaskGroup, Date, DateTo, Case):
    if type(Date) is datetime or type(DateTo) is datetime and \
       Case in set(['CO', 'CM']):
        ymd = Date.strftime('%Y%m%d')
        ymdTo = Date.strftime('%Y%m%d')
        base = pathlib.Path('.cache').joinpath(TaskGroup).joinpath(ymd)
        nc_files = list()
        for x in base.glob('LRC*/{}/*'.format(Case)):
            if Case == 'CO':
                t = 'DB/out/COMMON_OBJECT_CHECK_{}.csv'.format(ymdTo)
            else:
                t = 'DB/out/{}_CHECK_{}.csv'.format(x.name, ymdTo)
            try:
                x.parent.parent.joinpath(t).resolve()
            except:
                nc_files.append(x)
        return set(nc_files)
