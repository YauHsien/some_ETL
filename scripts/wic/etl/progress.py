import wic.etl.util.ftp as FTP
from wic.etl import stage
import datetime, re

## stages: for example, { stage.ctrl_object.update: None, stage.fm.update: None },
##         and keys are refered in following.
def start(stages, cus_ID, Date):
    if type(stages) is dict and type(cus_ID) is str and type(Date) is datetime.date:

        st = stage()
        
        for i, ln in FTP.generate_remote_files(cus_ID, Date):

            if stage.ctl_obj.update in stages and re_datasource.cmd
            
