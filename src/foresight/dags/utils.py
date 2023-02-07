import math
import json
import random
import datetime
import calendar
import itertools
import functools

import pandas as pd

random.seed(4)

@functools.cache
def get_gdelt_gkg_file_list():
    df = pd.read_csv("http://data.gdeltproject.org/gdeltv2/masterfilelist.txt", sep=' ', header=None)
    df['ts'] = df[2].str.extract(r'.+(\d{14}).+')
    df['ds'] = df[2].str[-11:-8]
    df['year'] = df[2].str.extract(r'.+(\d{4})\d{10}.+').fillna(0).astype('int')
    df = df.query("year >= 2020 & ds == 'gkg'")
    df['ts'] = pd.to_datetime(df['ts'])
    return df

def gdelt_files_between(start, end, p=.2):
    df = get_gdelt_gkg_file_list()
    files = df.loc[df['ts'].between(start, end, inclusive=False)]['ts'].dt.strftime('%Y%m%d%H%M%S') 
    return files.sample(frac=p)

def sample_between(start, end, p=.2, minutes=15):
    windows = list(dt.strftime('%Y%m%d%H%M%S') for dt in datetime_range(start, end, datetime.timedelta(minutes=minutes)))
    return random.sample(windows, int(len(windows)*p))

# map of month name to month number
month_map = dict([(m, n) for n, m in enumerate(calendar.month_name[1:], 1)])

# return quarter for given month number
quarter_map = lambda m: math.ceil(float(m) / 3)

def datetime_range(start, end, delta):
    current = start
    while current < end:
        yield current
        current += delta

def every_n_mins_between(start, end, minutes=15):
    return (dt.strftime('%Y%m%d%H%M%S') for dt in datetime_range(start, end, datetime.timedelta(minutes=minutes)))

class SerializableGenerator(list):
    """Generator that is serializable by JSON"""

    def __init__(self, iterable):
        tmp_body = iter(iterable)
        try:
            self._head = iter([next(tmp_body)])
            self.append(tmp_body)
        except StopIteration:
            self._head = []

    def __iter__(self):
        return itertools.chain(self._head, *self[:1])

def json_decode_many(s):
    # https://stackoverflow.com/a/68942444
    decoder = json.JSONDecoder()
    _w = json.decoder.WHITESPACE.match
    idx = 0
    while True:
        idx = _w(s, idx).end() # skip leading whitespace
        if idx >= len(s):
            break
        obj, idx = decoder.raw_decode(s, idx=idx)
        yield obj

def sample(iterable, n):
    """
    Returns @param n random items from @param iterable.

    Port of Knuth's reservoir sampling algorithm
    from https://stackoverflow.com/a/42532968
    """
    reservoir = []
    for t, item in enumerate(iterable):
        if t < n:
            reservoir.append(item)
        else:
            m = random.randint(0,t)
            if m < n:
                reservoir[m] = item
    return reservoir
