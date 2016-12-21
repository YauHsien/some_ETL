from pyparsing import Word
import pathlib, logging
import scripts.wic.etl.util.file


def analyze_SQL_tags(path):
    if type(path) is pathlib.Path:
        
        otSQL_str = '<SQL>'
        otSQL = Word(otSQL_str)
        otSQL_len = len(otSQL_str)
        ctSQL_str = '</SQL>'
        ctSQL = Word(ctSQL_str)
        ctSQL_len = len(ctSQL_str)

        bag = dict()
        for i, ln in gen_file_lines(path):
            ln1 = ln.rstrip()
            for result, start, end in otSQL.scanString(ln1):
                if end - start == otSQL_len:
                    bag[i+1, start+1] = otSQL_str
            for result, start, end in ctSQL.scanString(ln1):
                if end - start == ctSQL_len:
                    bag[i+1, start+1] = ctSQL_str

        #for k in sorted(bag):
        #    print(k, bag[k])
        return sorted(bag)



def determine_state(met_closed, open_loc, closed_loc, bag, k):

    if (met_closed, bag[k]) == (True, '<SQL>'):
        return True, False, k, closed_loc

    elif (met_closed, bag[k]) == (True, '</SQL>'):
        ## problem
        report(lvl= logging.ERROR, position= k, msg= 'Nested </SQL> not allowed')
        return False, met_closed, open_loc, k

    elif (met_closed, bag[k]) == (False, '<SQL>'):
        ## problem
        report(lvl= logging.ERROR, position= k, msg= 'Nested <SQL> not allowed')
        return False, met_closed, k, closed_loc

    elif (met_closed, bag[k]) == (False, '</SQL>'):
        return True, True, open_loc, k



def gen_SQL_list(path):
    if type(path) is pathlib.Path:

        bag = analyze_SQL_tags(path)
        flgen = gen_file_lines(path)

        met_closed = True
        open_loc = tuple()
        closed_loc = tuple()
        
        for k in bag:

            good, met_closed, open_loc, closed_loc = determine_state(met_closed,
                                                                     open_loc, closed_loc, bag, k)
            if good and pos_before(open_loc, closed_loc):
                yield take_SQL(flgen, open_loc, closed_loc)


def pos_before(open_loc, closed_loc):
    ln_open, cl_open = open_loc
    ln_closed, cl_closed = closed_loc
    return ln_open < ln_closed or (ln_open == ln_closed and cl_open < cl_closed)



def take_SQL(flgen, open_loc, closed_loc):

    ln_open, cl_open = open_loc
    ln_closed, cl_closed = closed_loc

    otSQL_len = len('<SQL>')
    str1 = ''

    for i, ln in flgen:
        ln = i + 1
        
        if ln_open == ln and ln_open == ln_closed:
            str1 = str1 + ln[(cl_open + otSQL_len - 1): (cl_closed - cl_open - otSQL_len - 1)]
            break
        
        elif ln_open == ln:
            str1 = str1 + ln[(cl_open + otSQL_len - 1):]
        
        elif ln_open < ln and ln < ln_closed:
            str1 = str1 + ln
        
        elif ln == ln_closed:
            str1 = str1 + ln[0: (cl_closed - otSQL_len - 1)]
            break

    return str1


for s in gen_SQL_list(pathlib.Path('priv/test/sql_module_source.txt')):
    print(s)
