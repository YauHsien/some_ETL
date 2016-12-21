import context
import wic.etl.util.regex as regex
import re

def test_left_most_inclusive():
    group1, next1, r = regex.left_most_inclusive('</sql>')
    m = re.compile('^{}(.*)$'.format(r))
    x = m.match('a<sql>b</sql>c</sql>d')
    assert x.__class__ is type(re.match("", ""))
    assert x.group(group1) == 'a<sql>b</sql>'
    assert x.group(next1) == 'c</sql>d'

def test_left_most_exclusive():
    group1, next1, r = regex.left_most_exclusive('</sql>')
    m = re.compile('^{}(.*)$'.format(r))
    x = m.match('a<sql>b</sql>c</sql>d')
    assert x.__class__ is type(re.match("", ""))
    assert x.group(group1) == 'a<sql>b'
    assert x.group(next1) == 'c</sql>d'

def test_not_include():
    group1, next1, r = regex.not_include('</sql>')
    m = re.compile('^{}(.*)'.format(r))
    x = m.match('ab</sql>b</sql>c')
    assert x.__class__ is type(re.match("", ""))
    assert x.group(group1) == 'ab'
    assert x.group(next1) == '</sql>b</sql>c'
