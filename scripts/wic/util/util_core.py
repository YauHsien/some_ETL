from wic.util import file as File
from wic.util import folder as Folder
from wic.util.logging import logger
import os
import datetime
import dateutil.parser as date_parser
import decimal
import random
import re
import socket
import csv
import time
import sys
import itertools
import inspect
import pathlib
import pkgutil

mysql_date_format = '%Y-%m-%d'
mysql_datetime_format = mysql_date_format + ' %H:%M:%S'

def valid_ip_address(ip):
    try:
        socket.inet_aton(ip)
        return True
    except socket.error:
        return False

def get_now_dt():
    return datetime.datetime.now().strftime(mysql_datetime_format)

def get_now_date():
    return datetime.datetime.now().strftime(mysql_date_format)

def camelcase_to_underscore(name):
    first_cap_re = re.compile('(.)([A-Z][a-z]+)')
    all_cap_re = re.compile('([a-z0-9])([A-Z])')
    s1 = first_cap_re.sub(r'\1_\2', name)
    return all_cap_re.sub(r'\1_\2', s1).lower()

def unique_list(items):
    s = set()
    return [x for x in items if not (x in s or s.add(x))]

def is_csv_file(csv_file):
    try:
        with open(csv_file) as csvfile:
            dialect = csv.Sniffer().sniff(csvfile.read(1024))
            csvfile.seek(0)
            return True
    except csv.Error:
        return False

def progress_bar(sleep=0.1, rng=101, short=False, show_percentage=True):
    for i in range(rng):
        time.sleep(sleep)
        num = 0
        if short:
            num = int(i / 10)
        else:
            num = i
        sys.stdout.write('\r{} [ {} ]'.format((str(i) + '%') if show_percentage else '', '#' * num))
        sys.stdout.flush()
    print()

def spinner(sleep=0.1, rng=100):
    for i, c in enumerate(itertools.cycle('/-\|')):
        if i == rng:
            break
        sys.stdout.write('\r' + c)
        sys.stdout.flush()
        time.sleep(sleep)

def is_numerical(data):
    x = 0
    try:
        x += data
        return True
    except TypeError:
        return False

def is_digit(data):
    try:
        return data.isdigit()
    except AttributeError:
        return False

def is_float(data):
    try:
        return str(float(data)) == str(data)
    except ValueError:
        return False

def pick(n, ls):
    if n >= len(ls):
        return ls
    limit = 3000
    while limit > 0:
        x = random.randrange(0, len(ls))
        y = random.randrange(0, len(ls))
        if x != y:
            t = ls[x]
            ls[x] = ls[y]
            ls[y] = t
        limit = limit - 1
    return ls[:n]
    
def decimal_random(start, stop, places):
    start *= (places * 10)
    stop *= (places * 10)
    places *= (places * 10)
    return float(decimal.Decimal(random.randrange(start, stop))/places)

def conv_date(condensed_date_str):
    if condensed_date_str is None:
        return None
    m = re.compile(r'^(\d\d\d\d)(\d\d)(\d\d)$')
    x = m.match(condensed_date_str)
    if x is None:
        return None
    try:
        return datetime.date(int(x.group(1)), int(x.group(2)), int(x.group(3)))
    except:
        return None

def is_mysql_dt(datetime_str):
    try:
        datetime.datetime.strptime(datetime_str, mysql_datetime_format)
        return True
    except ValueError:
        return False

def is_mysql_date(date_str):
    try:
        datetime.datetime.strptime(date_str, mysql_date_format)
        return True
    except ValueError:
        return False

def format_time_elapsed(seconds):
    seconds1 = int(seconds)
    tbase = datetime.datetime(1970, 1, 1)
    tdelta = datetime.timedelta(seconds= seconds1)
    t1 = tbase + tdelta
    h, m, s = t1.hour, t1.minute, t1.second
    if h is not 0:
        return '{}h {}m {}s'.format(h, m, s)
    elif m is not 0:
        return '{}m {}s'.format(m, s)
    elif s is not 0:
        return '{}s'.format(s)
    
"""
    @staticmethod
    def format_time_secs(seconds):
        seconds = int(seconds)
        minute, second = divmod(seconds, 60)
        hour, minute = divmod(minute, 60)
        # '{0:d}:{1:02d}:{2:02d}'.format(hour, minute, second)

        if minute and not hour:
            return '{}m {}s'.format(minute, second)
        elif hour:
            return '{}h {}m {}s'.format(hour, minute, second)
        else:
            return '{}s'.format(seconds)
"""
        
def extract_date(value):
    if type(value) is str:
        try:
            return date_parser.parse(value).date()
        except ValueError:
            try:
                return date_parser.parse(value, fuzzy=True).strftime(mysql_date_format)
            except ValueError:
                pass

    
def list_in_list(list1, list2):
    return len(list(set(list1).intersection(list2))) == len(list1)

    @staticmethod
    def remove_substring(str_text, substr):
        try:
            substr_re = re.compile(re.escape(substr)+'$')
            return substr_re.sub('', str_text)
        except:
            return False

def is_function(f):
    return hasattr(f, '__call__')

def get_arity(f):
    return len(inspect.getargspec(f).args)

def count_src(path):
    p = pathlib.Path(path).resolve()
    count = 0
    if File.is_path(p) and p.exists() and p.is_dir():
        for p1 in p.glob('*'):
            if p1.name != '__pycache__' and p1.is_dir():
                count = count + count_src(p1)
        for py in p.glob('*.py'):
            count1 = 0
            #logger(__name__).debug(py)
            if py.name == 'logging.py':
                continue
            with open(str(py), 'r') as fo:
                for ln in iter(fo):
                    line = ln.strip()
                    if line == '' or line.startswith('logger(__name__).debug(') or\
                       line.startswith('logger(mod_name).debug(') or\
                       line == '"""' or line.startswith('#'):
                        continue
                    count1 = count1 + 1
                fo.close()
            logger(__name__).debug('"{}": {}'.format(str(py), count1))
            count = count + count1
    logger(__name__).debug('"{}": {}'.format(str(p), count))
    return count

def has_module(fullpath):
    try:
        return pkgutil.find_loader(fullpath) is not None
    except:
        return False
