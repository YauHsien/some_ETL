from wic import RESTRICT
from wic.etl import key as SysKey
from .etl_core import *
config = get_computed_config(RESTRICT.CUSTOMER_ID,
                             SysKey.system_key[RESTRICT.TASKGROUP],
                             __name__)
