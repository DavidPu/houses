import json
import re

def get_keys(d):
    keys = []
    values = []
    if type(d) == dict:
        keys += d.keys()
        values = d.values()
    elif type(d) in [list, tuple]:
       values = d
    for v in values:
        keys += get_keys(v)
    return keys

react = json.load(open('InitialContext.json'))
dataCache = react['ReactServerAgent.cache']['dataCache']
print('\n'.join(dataCache.keys()))
print(dataCache['/stingray/api/home/details/aboveTheFold'])
results = dict()
for r in dataCache:
    v = dataCache[r]
    if v.get('res') and v['res'].get('text'):
        text = v['res']['text']
        results[r] = json.loads(text[len('{}&&'):])

with open('resuls.json', 'w+') as f:
    f.write(json.dumps(results))


def find_json_data(html):
    level = -1
    pos = []
    for i in range(len(html)):
        c = html[i]
        if c in ['{', '[']:
            if level == -1:
                level = 1
                pos.append(i-1)
            else:
                level += 1
        elif c in ['}', ']']:
            level -= 1
        if level == 0:
            pos.append(i+1)
            break
    return json.loads(html[pos[0]:pos[1]])


fname = r'html/ff3f1273a32de1439731ef630137bac6.html'
html = open(fname).read()
react = html.split('/stingray/api/home/details/belowTheFold')[1]
find_json_data(react)
