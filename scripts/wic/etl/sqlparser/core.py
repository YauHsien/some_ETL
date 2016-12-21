from wic import db as DB
from wic.db import template as SQLTemplate
from wic import util as Util
from wic.util import regex
from xml.dom import minidom as mdom
from xml.parsers.expat import ExpatError
import re, _io, copy, logging


def is_syntax_valid(str, tag_name):
    m = re.compile('(.*)<{}>.*'.format(tag_name.lower()))
    x = m.match(str.lower())
    if x is None:
        return False
    else:
        return m.match(x.group(1)) is None

## yield:
##  - Text ending with the tag </SQL>
def generate_string_ending_with_SQL_tag(fo):
    if fo.__class__ is _io.TextIOWrapper:
        group1, next1, r = regex.left_most_inclusive('</SQL>')
        m = re.compile('^{}(.*)$'.format(r))
        line = ''
        for ln in iter(fo):
            line1 = ln.strip() + ' '
            x = m.match(line1)
            if x is None:
                line = line + line1 if line1 != ' ' else line
            else:
                yield line + x.group(group1)
                line = x.group(next1)

def is_valid_SQL_tag(line):
    try:
        return mdom.parseString(line)
    except ExpatError as e:
        return '"{}"'.format(line) + e

def find_text_content_in_SQL_tag(tag):
    if type(tag) is mdom.Element and tag.tagName == 'SQL':
        for e in tag.childNodes:
            if type(e) is mdom.Text:
                str = e.data.strip()
                if str != '':
                    yield e.data.rstrip()

## yield:
##  - ('comment', arbitrary_text)
##  - ('sql', sql)
##  - ('error', error)
def generate_SQL_list(fo):
    if fo.__class__ is _io.TextIOWrapper:
        for ln in generate_string_ending_with_SQL_tag(fo):
            r = is_valid_SQL_tag(ln)
            if type(r) is mdom.Document:
                for x in r.getElementsByTagName('SQL'):
                    m1 = re.compile('^\s*#.*$')
                    for sql in find_text_content_in_SQL_tag(x):
                        if type(m1.match(sql)) is type(re.match("", "")):
                            yield 'comment', sql
                        else:
                            yield 'sql', sql
            else: # error
                yield 'error', r
                
## yield:
##  - ('comment', arbitrary_text)
##  - ('sql', sql)
##  - ('error', error)
##  - ('table', (database, table))
##  - ('bad_table', table)
def generate_table_yyyymm(inp):
    if type(inp) is _io.TextIOWrapper:
        for t, ln in generate_SQL_list(inp):
            if t == 'comment' or t == 'error':
                yield t, ln
            elif t == 'sql':
                for tt in generate_table_yyyymm(ln):
                    yield tt
                yield t, ln
    elif type(inp) is str:
        m1 = re.compile(r'^(.*\s+)([^\s]+)_yyyymm\s+.*$')
        m2 = re.compile(r'^([^.]+)[.]([^.]+)$')
        inp1 = copy.copy(inp)
        while True:
            x = m1.match(inp1)

            if x is None:
                break
            elif type(x) is type(re.match("", "")):
                inp1 = x.group(1)
                y = m2.match(x.group(2))
                if type(y) is type(re.match("", "")):
                    yield 'table', (y.group(1), y.group(2) + '_latest')
                else:
                    yield 'bad_table', x.group(2) 

# * * *
# * * *
# * * *
## yield:
##  - ('comment', arbitrary_text)
##  - ('sql', sql)
##  - ('error', error)
##  - ('table', (database, table))
def generate_lines_with_customer_tag_replaced(fo, cus_ID):
    if type(fo) is _io.TextIOWrapper:
        for t, ln in generate_table_yyyymm(fo):
            if t == 'sql':
                yield t, ln.replace('%%%', cus_ID)
            elif t == 'table':
                db, tbl = ln
                yield t, (db.replace('%%%', cus_ID), tbl)
            else:
                yield t, ln

def get_collection_and_make_database(fo, cus_ID):
    if type(fo) is _io.TextIOWrapper:
        proc, db_tbl_map = list(), dict()
        for t, ln in generate_lines_with_customer_tag_replaced(fo, cus_ID):
            if t == 'table':
                db, tbl = ln
                tbl1 = re.sub(r'_[lL][aA][tT][eE][sS][tT]$', '_yyyymm', tbl)
                tbl2 = SQLTemplate.find_table_with_latest_date(db, tbl)
                db_tbl_map[db, tbl1] = tbl2
            elif t == 'bad_table':
                logging.error('bad table `{}\''.format(ln))
            else:
                proc.append((t, ln))
        db_tbl_map = SQLTemplate.ensure_tables(db_tbl_map)
        return proc, db_tbl_map

## yield:
##  - ('info' / 'error', 'comment' / 'sql' / 'error', message)
def generate_sql_result(fo, cus_ID):
    if type(fo) is _io.TextIOWrapper:
        proc, tmap = get_collection_and_make_database(fo, cus_ID)
        #logging.warning('internal: {}'.format(tmap))
        for i, (t, ln) in enumerate(proc):
            if t == 'comment':
                yield 'info', t, ln
            elif t == 'sql':
                sql1 = ln
                db_tbl_list = regex.find_all_db_dot_tbl(sql1)
                #logging.warning('internal: {}'.format(db_tbl_list))
                for _, (db, tbl1) in enumerate(db_tbl_list):
                    if tmap[db, tbl1] is not None:
                        _, tbl2 = tmap[db, tbl1]
                        sql1 = sql1.replace('{}.{}'.format(db, tbl1), '{}.{}'.format(db, tbl2))
                #logging.warning('internal: {}'.format(sql1))
                if DB.run_sql(sql1):
                    yield 'info', t, sql1
                else:
                    yield 'error', t, sql1
            elif t == 'error':
                yield 'error', 'error', sql1

def work_and_log(fo, cus_ID):
    if type(fo) is _io.TextIOWrapper:
        for n, t, m in generate_sql_result(fo, cus_ID):
            if n == 'info':
                logging.info('[{}] {}'.format(t, m))
            if n == 'error':
                logging.error('[*{}] {}'.format(t, m))
