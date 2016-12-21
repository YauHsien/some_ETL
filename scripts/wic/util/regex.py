import re
import copy

m1 = re.compile(r'^(.*\s+)([^\s]+)_yyyymm\s+.*$')
m2 = re.compile(r'^([^.]+)[.]([^.]+)$')
m3 = re.compile(r'^(.*)\s+([^\s.]+)[.]([^\s.]+_yyyymm)(\s|[;])*.*$')

def find_all_db_dot_tbl(str):
    db_tbl_list = list()
    str1 = copy.copy(str)
    while True:
        x = m3.match(str1)
        if x is None:
            break
        else:
            str1 = x.group(1)
            db_tbl_list.append((x.group(2), x.group(3)))
    return db_tbl_list

def left_most_inclusive(term):
    group_index, next_group, r = not_include(term)
    # It prepends a new group in front of those `not_include` groups.
    return group_index, next_group + 1, '({0}{1})'.format(r, term)

def left_most_exclusive(term):
    group_index, next_group, r = not_include(term)
    # It takes the first group of those `not_include` groups.
    return group_index + 1, next_group + 1, '({0}{1})'.format(r, term)

def not_include(term):
    group_index = 1
    next_group = 3
    return group_index, next_group, '(((?!{}).)*)'.format(term)
