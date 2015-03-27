#! /usr/bin/env python

import colorsys
import math
import random


class ColorScheme(object):
    def __init__(self):
        pass

    def get(self, name):
        raise NotImplementedError()

    def get_as_float(self, name):
        return self.get(name)

    def get_as_int(self, name):
        r, g, b = self.get(name)
        return int(r*255), int(g*255), int(b*255),

    def get_as_hex(self, name):
        rgb = self.get_as_int(name)
        return "%02x%02x%02x" % rgb


class RandomCS(ColorScheme):
    def __init__(self, seed=None):
        self.col = {}
        self.rnd = random.Random(seed)

    def get(self, name):
        if name in self.col:
            col = self.col[name]
        else:
            self.col[name] = col = self.rnd.random(), self.rnd.random(), self.rnd.random()
        return col


def _ngrams(n, s):
    return {s[i:i+n] for i in range(len(s)-n+1)}


def jaccard(ng1, ng2):
    lu = len(ng1 | ng2)
    return len(ng1 & ng2)/float(lu) if lu > 0 else 0


class ProxCS(ColorScheme):
    def __init__(self, seed=None, lmin=0.3, lmax=2.8):
        self.rnd = random.Random(seed)
        self.cols = {}
        self.tri = {}
        self.lmin = lmin
        self.lmax = lmax

    def get(self, name):
        name = name.lower()
        if name in self.cols:
            return self.cols[name]

        tri = _ngrams(3, name)

        dmin, nmin = 1, None
        for n, t in self.tri.iteritems():
            d = 1-jaccard(t, tri)
            if d <= dmin:
                dmin = d
                nmin = n
        if dmin >= 1:
            col = tuple(self.rnd.random() for i in range(3))
        else:
            stcol = self.cols[nmin]
            theta = self.rnd.random()*2*math.pi
            phi = (self.rnd.random()-0.5)*math.pi
            dxyz = (math.sin(phi)*math.cos(theta), math.sin(phi)*math.sin(theta), math.cos(phi))
            col = tuple(min(1, max(0, c+d*dmin**2)) for c, d in zip(stcol, dxyz))

        lum = sum(col)
        if lum < self.lmin:
            corr = (self.lmin-lum)/3
        elif lum > self.lmax:
            corr = (lum-self.lmax)/3
        else:
            corr = 0
        col = tuple(min(1, max(0, (v+corr))) for v in col)

        self.cols[name] = col
        self.tri[name] = tri
        return col


class PrefixCS(ColorScheme):
    """
    Consider only the first prefixlen name bytes.
    If the prefixes are known beforehand, it may be passed to init.
    """
    def __init__(self, prefixlen=4, prefixes=None, seed=None):
        self.rnd = random.Random(seed)
        self.prefixlen = prefixlen
        #: prefix -> hue
        self.hues = dict()
        if prefixes:
            nb = len(prefixes)
            for i, prefix in enumerate(prefixes):
                self.hues[prefix] = i/nb
        #: (prefix, saturation) -> r, g, b (saturation: int, 0 -> 100)
        self.colorcache = dict()

    def getfromcache(self, prefix, saturation):
        val = self.colorcache.get((prefix, saturation), None)
        if not val:
            val = colorsys.hsv_to_rgb(self.hues[prefix], saturation, 1.00)
            self.colorcache[(prefix, saturation)] = val
        return val

    def get(self, name, saturation=1):
        prefix = name[:self.prefixlen]
        if prefix not in self.hues:
            self.hues[prefix] = self.rnd.random()

        return self.getfromcache(prefix, saturation)

    def get_as_float(self, name, saturation=1):
        return self.get(name, saturation)

    def get_as_int(self, name, saturation=1):
        r, g, b = self.get(name, saturation)
        return int(r*255), int(g*255), int(b*255),

    def get_as_hex(self, name, saturation=1):
        rgb = self.get_as_int(name, saturation)
        return "%02x%02x%02x" % rgb
