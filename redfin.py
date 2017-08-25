#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
Google map drive distance:
https://maps.googleapis.com/maps/api/distancematrix/json?origins=Falkirk%20Dr,SAN%20JOSE,CA,95135&destinations=2701%20San%20Tomas%20Expy,%20Santa%20Clara,%20CA%2095050&mode=driving
https://stackoverflow.com/questions/29003118/get-driving-distance-between-two-points-using-google-maps-api
'''

import csv
import os
import re
import md5
import json
import requests
import requests_cache
from bs4 import BeautifulSoup
'''
requests_cache.install_cache(os.path.join(os.path.dirname(__file__),
                            'redfin_cache'))
'''
if requests.__version__ < '2.2.4':
    requests.packages.urllib3.disable_warnings()

UA_HEADER = 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36'

# resp = redfin.get('/county/345/CA/Santa-Clara-County/filter/sort=lo-days,property-type=house+condo+townhouse,min-price=450k,max-price=900k')
# dl_all = '/stingray/api/gis-csv?al=3&market=sanfrancisco&max_price=900000&min_price=450000&num_homes=350&ord=days-on-redfin-asc&page_number=1&region_id=345&region_type=5&sf=1,2,3,5,6,7&sp=true&status=9&uipt=1,2,3&v=8&zoomLevel=8'
dl_all = {
    '/stingray/api/gis-csv?al=3&market=sanfrancisco&max_price=900000&min_price=450000&ord=days-on-redfin-asc&page_number=1&region_id=345&region_type=5&sf=1,2,3,5,6,7&sp=true&status=9&uipt=1,2,3&v=8',
    '/stingray/api/gis-csv?al=3&market=sanfrancisco&max_price=900000&min_price=450000&ord=days-on-redfin-asc&page_number=1&region_id=303&region_type=5&sf=1,2,3,5,6,7&sp=true&status=9&uipt=1,2,3&v=8',
    '/stingray/api/gis-csv?al=3&market=sanfrancisco&max_price=900000&min_price=450000&ord=days-on-redfin-asc&page_number=1&region_id=343&region_type=5&sf=1,2,3,5,6,7&sp=true&status=9&uipt=1,2,3&v=8'
}

class Redfin(object):
    def __init__(self):
        self.baseurl = 'https://www.redfin.com'
        self.session = requests.Session()
        self.session.headers['User-Agent'] = UA_HEADER
        self.session.headers['origin'] = self.baseurl
        self.session.headers['Referer'] = self.baseurl

    def _url(self, url):
        return self.baseurl + url if url[0] == '/' else url

    def get(self, url, **kwargs):
        return self.session.get(self._url(url), verify=False, **kwargs)

    def post(self, url, **kwargs):
        return self.session.post(self._url(url), verify=False, **kwargs)

    def save_cookies(self):
        with open('cookies.json', 'w+') as f:
            f.write(json.dumps(self.session.cookies.items()))

    def _restore_cookies(self):
        if not os.path.exists('cookies.json'):
            return 400

        cookies = json.load(open('cookies.json'))
        for c in cookies:
            self.session.cookies.set(c[0], c[1])
        resp = self.get(dl_all[0])
        if resp.status_code != 200:
            self.session.cookies.clear()
            return 400
        else:
            return 200

    def login(self, user, pwd, cookies=None):
        if self._restore_cookies() == 200:
            print('re-use cookie')
            return 200

        resp = self.post(
                         '/stingray/do/api-login',
                         data={'email': user, 'pwd': pwd})
        print(resp.content[4:])
        print('cookie:', self.session.cookies.items())
        if resp.status_code == 200:
            self.save_cookies()
        return resp.status_code


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


class CsvData_OLD(object):
    def __init__(self, data):
        data = data.strip('\n').strip('\r').split('\n')
        self.header = data[0].strip('\n').strip('\r').split(',')
        self.data = list(csv.reader(data[1:]))
        self.field_map = dict()
        for idx in range(len(self.header)):
            self.field_map[self.header[idx]] = idx

    def get_header(self):
        return self.header

    def iter_by_fields(self, *args):
        sub_csv = list()
        for name in args:
            assert(name in self.field_map)
            idx = self.field_map[name]
            sub_csv.append(map(lambda row: row[idx], self.data))
        return zip(*sub_csv)


def details(html):
    soup = BeautifulSoup(html, 'lxml')
    info = dict()

    def schools(html):
        sch = soup.find('div', {'class': 'schools-content'})
        if sch:
            table = sch.find('table', {'class': 'basic-table-2'})
            sch_names = map(lambda a: (a.text, a['href']),
                            table.find_all('a', {'class': 'school-name'}))
            sch_rates = map(lambda d: d.text,
                            table.find_all('div', {'class': 'gs-rating'}))
            distances = map(lambda c: c.text,
                            table.find_all('td', {'class': 'distance-col'}))

            info['schools'] = zip([n[0] for n in sch_names],
                                  [n[1] for n in sch_names],
                                  sch_rates, distances)

    def property_details(html):
        prop = soup.find('div', {'data-rf-test-id': 'propertyDetails'})
        if prop:
            prop_desc = '\n'.join(map(lambda d: d.text, prop.find_all('div')))
            info['prop_details'] = prop_desc
            info['is_senior'] = prop_desc.find('Senior Community') > 0 or \
                prop_desc.find('55+') > 0

    def react_json(html):
        root = 'root.__reactServerState.InitialContext'
        initctx = filter(lambda l: l.startswith(root), html.split('\n'))
        if initctx:
            initctx = initctx[0]
            js = initctx[initctx.find('{'):]
            with open('InitialContext.json', 'w+') as f:
                f.write(js)
            ctx = json.loads(js[:-1])
            datacache = ctx['ReactServerAgent.cache']['dataCache']
            key = filter(lambda v: v.find('belowTheFold') > 0,
                         datacache.keys())
            if key:
                key = key[0]
                if 'res' in datacache[key]:
                    prop_info = json.loads(datacache[key]['res']['text'][4:])
                    prop_info = prop_info['payload']
                    info['prop_info_json'] = prop_info

    schools(html)
    property_details(html)
    react_json(html)
    return info


# return tuple: ([cvs header], [cvs data])
def download_one_cvs(redfin, url):
    resp = redfin.get(url)
    assert(resp.status_code == 200)
    data = re.sub(r'URL \([^\)]+\)', 'URL', resp.content)
    data = data.strip('\n').strip('\r').split('\n')
    csv_header = data[0].strip('\n').strip('\r').split(',')
    csv_data = list(csv.reader(data[1:]))
    return (csv_header, csv_data)


def get_cvs(redfin):
    resp = redfin.get('/county/345/CA/Santa-Clara-County/filter/sort=lo-days,property-type=house+condo+townhouse,min-price=450k,max-price=900k')
    with open('all.html', 'w+') as f:
        f.write(resp.content)

    all_csv = map(download_one_cvs, dl_all)
    csv_header = all_csv[0][0]
    all_csv = map(lambda d: d[1:], all_csv)
    csv_data = reduce(lambda l, r: l + r, all_csv)
    with open('download_all.csv', 'w+') as f:
        f.write(all_csv[0][0])

    data = data.strip('\n').strip('\r').split('\n')
    csv_header = data[0].strip('\n').strip('\r').split(',')
    csv_data = list(csv.reader(data[1:]))
    csvdata = CsvData(csv_header, csv_data)
    urls = [c[0] for c in csvdata.iter_by_fields('URL')]
    data_store = dict()
    data_store['csv_header'] = csv_header
    data_store['csv_data'] = csv_data
    data_store['details'] = dict()
    for url in urls:
        url = url.split('redfin.com')[1] if url.find('redfin.com') >= 0 else url
        data_store['details'][url] = details(fetch_details(redfin, url))

    with open('redfin_data_store.json', 'w+') as f:
        f.write(json.dumps(data_store))


def fetch_details(redfin, url):
    print(url)
    resp = redfin.get(url)
    assert(resp.status_code == 200)
    html = resp.content
    # use path instead:
    if url.find('redfin.com') > 0:
        url = url.split('redfin.com')[1]
    print(url)
    fname = md5.md5(url).hexdigest()
    with open('html/%s.html' % fname, 'w+') as f:
        f.write(html)

    return html


def test_details():
    url = '/CA/Los-Gatos/443-Alberto-Way-95032/unit-B215/home/586935'
    fname = md5.md5(url).hexdigest()
    html = open('html/' + fname + '.html').read()
    print(details(html))

# test_details()
redfin = Redfin()
status = redfin.login('pulq@163.com', '@163.Com')
print('login status:', status)
assert(status == 200)
get_cvs(redfin)
redfin.save_cookies()
