from wic import default as Default
from wic.util import file as File
from wic.util.logging import logger
import sys



def create(path, mod_name):
    if File.is_path(path):
        if path.exists():
            logger(mod_name).info('found: "{}"'.format(str(path)))
        else:
            logger(mod_name).info('creating folder \"{}\"'.format(str(path)))
            path.mkdir(parents= True, exist_ok= True)


            
def remove(path, mod_name):
    if File.is_path(path):
        for x in path.glob('*'):
            if x.is_dir():
                remove(x, mod_name)
            else:
                File.remove(x, mod_name)
        path.rmdir()
        logger(mod_name).info('remove: "{}"'.format(path))



def is_empty(path, mod_name):
    if File.is_path(path):
        return [] == [ x for x in path.iterdir() ]
