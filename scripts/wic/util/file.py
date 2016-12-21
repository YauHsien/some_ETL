from .logging import *
import wic.default as Default
import pathlib, zipfile, json, time

def is_path(path):
    pt = type(path)
    return pt in [pathlib.Path, pathlib.PosixPath, pathlib.WindowsPath]

def exists(path):
    if type(path) is str:
        try:
            return pathlib.Path(path).resolve().exists()
        except:
            return False
    elif is_path(path):
        try:
            return path.resolve().exists()
        except:
            return False

## return:
##  - True
##  - False
##  - Exception
def is_file(path):
    try:
        return is_path(path) and path.resolve().is_file()
    except Exception as er:
        return er
    
def gen_file_lines(path, newline= '\n', encoding= 'utf-8'):
    if is_path(path):
        with open(str(path.resolve()), 'r',
                  newline= newline, encoding= encoding) as f:
            for i, ln in enumerate(f):
                yield i, ln.rstrip()

def create(path, module_name):
    logger(module_name).debug('creating file \"{}\"'.format(path))
    try:
        path.touch()
    except Exception as e:
        errmsg = 'bad file creation: {}'.format(e)
        logger(__name__).error(errmsg)
        sys.exit(Default.INTERRUPT)

def remove(path, mod_name):
    if is_path(path) and exists(path):
        path.unlink()
        logger(mod_name).debug('remove: "{}"'.format(path))

def check_zipfile(target, mod_name):
    logger(mod_name).info('check: \"{}\"'.format(target))
    try:
        zf = zipfile.ZipFile(target)
        zf.testzip()
        zf.close()
        return True
    except Exception as e:
        logger(__name__).debug('bad ZIP: {}'.format(e))
        return False

def load_JSON(path, mod_name):
    if is_path(path):
        with open(str(path), 'r') as fo:
            json_data = json.load(fo)
            fo.close()
        return json_data

def dump_JSON(path, json_data, mod_name):
    if is_path(path):
        with open(str(path), 'w') as fo:
            fo.write(json.dumps(json_data, sort_keys= True, indent= 4))
            fo.close()
        logger(mod_name).info('written: "{}"'.format(str(path)))
