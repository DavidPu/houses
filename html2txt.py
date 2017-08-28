#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
from bs4 import BeautifulSoup

html = open(os.sys.argv[1]).read()
soup = BeautifulSoup(html, 'lxml')
for script in soup(['script', 'style']):
    script.extract()
text = soup.get_text()
lines = (line.strip() for line in text.splitlines())
chunks = (phrase.strip() for line in lines for phrase in line.split(" "))
text = u'\n'.join(chunk for chunk in chunks if chunk)
with open('text.txt', 'w+b') as f:
    f.write(text.encode('utf-8'))
