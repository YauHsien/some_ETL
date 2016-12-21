import sys, os, pathlib

OS = 'WIN' if sys.platform.startswith('win') else \
     'LINUX' if sys.platform.startswith('linux') else \
     'OTHER'
CONFIG_PATH = pathlib.Path(os.getenv('appdata')) if OS == 'WIN' else \
              pathlib.Path.home() if OS == 'LINUX' else \
              pathlib.Path.home()
CONFIG_FOLDER = 'XiNOS' if OS == 'WIN' else \
                '.xinos' if OS == 'LINUX' else \
                '.xinos'
#DATA_PATH = '/media/yauhsien/TOSHIBA EXT/data'
DATA_PATH = 'E:\\data'
OK = 'OK'
INTERRUPT = 'INTERRUPT'
LOG_FMT = '%(asctime)-15s [%(levelname)s] %(name)s: %(message)s'
