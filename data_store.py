#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import csv
import sys
import os
import math
import time

import requests
import requests_cache
requests_cache.install_cache(os.path.join(os.path.dirname(__file__),
                             'redfin_cache'))
if sys.version_info[0] == 3:
    import urllib.parse as uparse
else:
    import urllib as uparse


if requests.__version__ < '2.2.4':
    requests.packages.urllib3.disable_warnings()


class CsvData(object):
    def __init__(self, header, data):
        self.header = header
        self.data = data
        self.field_map = dict()
        for idx in range(len(self.header)):
            self.field_map[self.header[idx]] = idx

    def get_header(self):
        return self.header

    def iter_by_fields(self, *args):
        sub_csv = list()
        print(args)
        for name in args:
            assert(name in self.field_map)
            idx = self.field_map[name]
            sub_csv.append([row[idx] for row in self.data])
        # return list(array) of list instead of tuple(zip returned by default)
        return map(list, zip(*sub_csv))


# elementary school name, score and distance
def sch_info(ds, url):
    details = ds['details']
    prop = details[url]
    if prop.get('schools'):
        elementary = prop['schools'][0]
        return [elementary[0], elementary[2], elementary[3].split(' ')[0]]
    else:
        return ['', '', '']


def extend_school_info(ds):
    csvdata = CsvData(ds['csv_header'], ds['csv_data'])
    urls = map(lambda u: u[0].split('redfin.com')[1] if u[0].find('redfin.com') >= 0 else u[0], csvdata.iter_by_fields('URL'))
    csv_header = ds['csv_header'] + ['SCHOOL NAME', 'SCHOOL SCORE', 'SCHOOL DISTANCE']
    csv_data = ds['csv_data']
    url_idx = csv_header.index('URL')
    for i in range(len(csv_data)):
        url = csv_data[i][url_idx].split('redfin.com')[1]
        csv_data[i].extend(sch_info(ds, url))

    print(csv_header)
    print(csv_data[0])
    ds['csv_header'] = csv_header
    ds['csv_data'] = csv_data
    return ds


# https://developers.google.com/maps/documentation/distance-matrix/intro
def drive_info(origins,
               dest=['2701 San Tomas Expy, Santa Clara, CA 95050'],
               rate_limit_sec=3):
    url = 'https://maps.googleapis.com/maps/api/distancematrix/json?'
    param = {
             'origins': '|'.join(origins),
             'destinations': dest,
             'mode': 'driving'
    }
    url = url + uparse.urlencode(param)
    res = requests.get(url)
    assert(res.status_code == 200)
    time.sleep(rate_limit_sec)
    return res.json()

def extend_distance_matrix(ds):
    new_csv = CsvData(ds['csv_header'], ds['csv_data'])
    addrs = new_csv.iter_by_fields('URL', 'ADDRESS', 'CITY', 'STATE', 'ZIP', 'LATITUDE', 'LONGITUDE')
    lats = list(map(lambda addr: ','.join(list(addr)[-2:]), addrs))
    # origins limit per request according to  google distance-matrix
    stride = 25
    drvs = [lats[i*stride:(i+1)*stride] for i in range(int((len(lats)+stride-1)/stride))]
    drvinfos = list(map(drive_info, drvs))
    # print(drvinfos)
    # with open('drvinfos.json', 'w+') as f:
    #     f.write(json.dumps(drvinfos))
    # drvinfos = json.load(open('drvinfos.json'))
    cnt = 0
    for group in drvinfos:
        assert(group['status'] == 'OK')
        rows = group['rows']
        for row in rows:
            el = row['elements'][0]
            assert(el['status'] == 'OK')
            dist = el['distance']['text'].split(' ')[0]
            duration =  el['duration']['text'].split(' ')[0]
            addrs[cnt] += (dist, duration)
            ds['csv_data'][cnt] += (dist, duration)
            cnt += 1

    with open('address_distance_matrix.json', 'w+') as f:
        f.write(json.dumps(addrs))

    print('addrs len:%d' % len(addrs), 'drv info len:%d' % cnt)
    ds['csv_header'].extend(['DRIVE DISTANCE(KM)', 'DRIVE DURATION(MIN)'])
    return ds


def extend_is_senior(ds):
    def url_strip(u):
        return u.split('redfin.com')[1] if u.find('redfin.com') > 0 else u

    csvdata = CsvData(ds['csv_header'], ds['csv_data'])
    urls = map(lambda u: u[0], csvdata.iter_by_fields('URL'))
    urls = map(url_strip, urls)
    csv_header = ds['csv_header'].extend(['IS SENIOR'])
    csv_header = ds['csv_header']
    csv_data = ds['csv_data']
    url_idx = csv_header.index('URL')
    details = ds['details']
    for i in range(len(csv_data)):
        url = url_strip(csv_data[i][url_idx])
        print(url)
        csv_data[i].extend(
            'Y' if url in details and details[url].get('is_senior') else 'N')

    print(csv_header)
    print(ds['csv_header'])
    print(csv_data[0])
    print(ds['csv_data'][0])


data_store = json.load(open('redfin_data_store.json'))
extend_distance_matrix(extend_school_info(data_store))

with open('redfin_data_store_processed.json', 'w+') as f:
    f.write(json.dumps(data_store))

data_store = json.load(open('redfin_data_store_processed.json'))
print(data_store['csv_header'])
print(data_store['csv_data'][0])
extend_is_senior(data_store)
with open('redfin_data_store_processed_is_senior.json', 'w+') as f:
    f.write(json.dumps(data_store))


with open('redfin_prop_info.csv', 'w+t') as f:
    csv_out = csv.writer(f, quoting=csv.QUOTE_NONNUMERIC)
    csv_out.writerow(data_store['csv_header'])
    map(csv_out.writerow, data_store['csv_data'])

# only keep below fileds:
interested_fields = (
    "URL",
    "IS SENIOR",
    "PRICE",
    "HOA/MONTH",
    "SCHOOL SCORE",
    "SCHOOL DISTANCE",
    "SCHOOL NAME",
    "DRIVE DISTANCE(KM)",
    "DRIVE DURATION(MIN)",
    "DAYS ON MARKET",
    "PROPERTY TYPE",
    "CITY",
    "LOCATION",
    "ZIP",
    "BEDS",
    "BATHS",
    "SQUARE FEET",
    "YEAR BUILT",
    "FAVORITE",
    "NEXT OPEN HOUSE START TIME",
    "NEXT OPEN HOUSE END TIME"
)
with open('redfin_prop_info_clean.csv', 'w+t') as f:
    csvd = CsvData(data_store['csv_header'], data_store['csv_data'])
    new_ds = csvd.iter_by_fields(*interested_fields)
    csv_out = csv.writer(f, quoting=csv.QUOTE_NONNUMERIC)
    csv_out.writerow(interested_fields)
    map(csv_out.writerow, new_ds)
    print(interested_fields)
    print(new_ds[0])
