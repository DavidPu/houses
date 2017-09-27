#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import md5
import time


class cache(object):
    HEADER_SIZE = 36

    def __init__(self, folder='pycache'):
        if not os.path.exists(folder):
            os.makedirs(folder)
        self.folder = folder

    def _header(self, expire):
        t = int(expire + time.time())
        header = hex(t)
        header += '\0' * (cache.HEADER_SIZE - len(header))
        return header

    def _is_expired(self, header):
        # 0xDDDDDDDD
        t = int(header[0:10], base=16)
        return t < time.time()

    def put(self, key, value, expire_secs=20*60):
        key = md5.md5(key).hexdigest()
        fname = os.path.join(self.folder, key)
        with open(fname, 'w+b') as f:
            f.write(self._header(expire_secs))
            f.write(value)

    def get(self, key):
        key = md5.md5(key).hexdigest()
        fname = os.path.join(self.folder, key)
        if os.path.exists(fname):
            ret = open(fname).read()
            hdr = ret[:self.HEADER_SIZE]
            return ret[self.HEADER_SIZE:] if not self._is_expired(hdr) else None
        else:
            return None

    def delete(self, key):
        key = md5.md5(key).hexdigest()
        fname = os.path.join(self.folder, key)
        if os.path.exists(fname):
            os.remove(fname)

    def invalidate(self):
        pass


if __name__ == '__main__':
    kache = cache()
    csvdata = kache.get('data:csv')
    print(csvdata.keys())
