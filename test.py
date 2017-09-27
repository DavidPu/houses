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
from bs4 import BeautifulSoup

import cachedb

'''
import requests_cache
requests_cache.install_cache(os.path.join(os.path.dirname(__file__),
                            'redfin_cache'))
'''

if requests.__version__ < '2.2.4':
    requests.packages.urllib3.disable_warnings()

UA_HEADER = 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36'

# resp = redfin.get('/county/345/CA/Santa-Clara-County/filter/sort=lo-days,property-type=house+condo+townhouse,min-price=450k,max-price=900k')
# dl_all = '/stingray/api/gis-csv?al=3&market=sanfrancisco&max_price=900000&min_price=450000&num_homes=350&ord=days-on-redfin-asc&page_number=1&region_id=345&region_type=5&sf=1,2,3,5,6,7&sp=true&status=9&uipt=1,2,3&v=8&zoomLevel=8'
dl_all = [
    '/stingray/api/gis-csv?al=3&market=sanfrancisco&max_price=900000&min_price=450000&ord=days-on-redfin-asc&page_number=1&region_id=345&region_type=5&sf=1,2,3,5,6,7&sp=true&status=9&uipt=1,2,3&v=8',
    '/stingray/api/gis-csv?al=3&market=sanfrancisco&max_price=900000&min_price=450000&ord=days-on-redfin-asc&page_number=1&region_id=303&region_type=5&sf=1,2,3,5,6,7&sp=true&status=9&uipt=1,2,3&v=8',
    '/stingray/api/gis-csv?al=3&market=sanfrancisco&max_price=900000&min_price=450000&ord=days-on-redfin-asc&page_number=1&region_id=343&region_type=5&sf=1,2,3,5,6,7&sp=true&status=9&uipt=1,2,3&v=8'
]


def all_keys(d):
    keys = d.keys()
    for k in map(lambda v: all_keys(v),
                 filter(lambda t: type(t) == dict, d.values())):
        keys.extend(k)
    return keys


def get_values(d, key):
    if not d:
        return []

    results = []
    if key in d:
        results.append(d[key])
    for val in map(lambda v: get_values(v, key),
                   filter(lambda t: type(t) is dict, d.values())):
        for v in val:
            if v:
                print("VVVV:", type(v))
                results.append(v)
    if results:
        print("DDDDD:", type(results[0]))
    return results


class Redfin(object):
    def __init__(self):
        self.baseurl = 'https://www.redfin.com'
        self.session = requests.Session()
        self.session.headers['User-Agent'] = UA_HEADER
        self.session.headers['origin'] = self.baseurl
        self.session.headers['Referer'] = self.baseurl
        self.kache = cachedb.cache()

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
        for name in args:
            assert(name in self.field_map)
            idx = self.field_map[name]
            sub_csv.append([row[idx] for row in self.data])
        # return list(array) of list instead of tuple(zip returned by default)
        return map(list, zip(*sub_csv))


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
            print(info['schools'])
        else:
            print("NO SCHOOL INFO")

    def property_details(html):
        for script in soup(['script', 'style']):
            script.extract()
        prop_desc = soup.get_text()
        info['prop_details'] = prop_desc
        info['is_senior'] = prop_desc.find('Senior Community') > 0 or \
                prop_desc.find('55+') > 0

    def react_json(html):
        root = 'root.__reactServerState.InitialContext'
        initctx = filter(lambda l: l.startswith(root), html.split('\n'))
        if initctx:
            initctx = initctx[0]
            js = initctx[initctx.find('{'):-1]
            with open('InitialContext.json', 'w+') as f:
                f.write(js)
            ctx = json.loads(js)
            ctx = get_values(ctx, 'dataCache')[0]
            results = dict()
            for k, v in ctx.iteritems():
                text = get_values(v, 'text')
                print(len(text))
                if text:
                    text = json.loads(text[0][len('{}&&'):].strip(','))
                    results[k] = text

            info['react_data'] = results
            with open('react_data.json', 'w+') as f:
                f.write(json.dumps(results))

    def elementary_sch(s):
        if 'K' in s.get('gradeRanges').upper():
            return True
        if 'elementary' in s.get('name').lower():
            return True
        else:
            return False

    schools(html)
    property_details(html)
    react_json(html)
    if 'react_data' in info:
        propjson = info['react_data']
        keys = all_keys(propjson)
        sch = get_values(propjson, 'servingThisHomeSchools')
        print(sch)
        assert 0
        elem = filter(elementary_sch, sch)
        if elem:
            elem.sort(key=lambda e: int(e['greatSchoolsRating']))
            # elementary school name, score and distance
            info['schools'] = (elem[0].get('name'),
                               elem[0].get('greatSchoolsRating'),
                               elem[0].get('distanceInMiles'))
            print(info['schools'])
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


def to_json(**kwargs):
    return json.dumps(kwargs)


def get_cvs(redfin):
    def _dl_one_cvs(url):
        return download_one_cvs(redfin, url)

    resp = redfin.get('/county/345/CA/Santa-Clara-County/filter/sort=lo-days,property-type=house+condo+townhouse,min-price=450k,max-price=900k')
    with open('all.html', 'w+') as f:
        f.write(resp.content)

    all_csv = map(_dl_one_cvs, dl_all)
    csv_header = all_csv[0][0]
    csv_data = list()
    for c in all_csv:
        csv_data += c[1]

    kache = cachedb.cache()
    kache.put('data:csv',
              to_json(csv_header=csv_header, csv_data=csv_data))
    with open('download_all.csv', 'w+') as f:
        csv_out = csv.writer(f, quoting=csv.QUOTE_NONNUMERIC)
        csv_out.writerow(csv_header)
        map(csv_out.writerow, csv_data)

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
    # use path instead:
    u = url
    if url.find('redfin.com') > 0:
        u = url.split('redfin.com')[1]
    print(u)
    fname = '%s.html' % md5.md5(u).hexdigest()
    fname = os.path.join('html', fname)
    if os.path.exists(fname):
        print("re-use pre-exiting HTML", fname)
        return open(fname).read()
    else:
        resp = redfin.get(url)
        assert(resp.status_code == 200)
        resp.encoding = 'utf-8'
        html = resp.content
        with open(fname, mode='w+') as f:
            f.write(html)

    return html


def test_details():
    # url = '/CA/Milpitas/1605-Grand-Teton-Dr-95035/home/657528'
    url = '/CA/San-Jose/2202-Olivegate-Ln-95136/home/982929'
    fname = md5.md5(url).hexdigest()
    html = open('html/' + fname + '.html').read()
    details(html)

test_details()
# redfin = Redfin()
# status = redfin.login(os.environ.get('REDFIN_USER'), os.environ.get('REDFIN_PWD'))
# print('login status:', status)
# assert(status == 200)
# get_cvs(redfin)
#
# redfin.save_cookies()
