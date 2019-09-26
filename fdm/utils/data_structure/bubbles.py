from datetime import datetime
from datetime import timedelta


class TimeBubble:
    def __init__(self, lower, upper):
        if lower > upper:
            raise ValueError(
                'Upper {} is less then lower {}.'.format(upper, lower))
        self.max = upper
        self.min = lower
        self.mid = self.max.timestamp() + self.min.timestamp()
        self.leg = self.max.timestamp() - self.min.timestamp()

    def __repr__(self):
        start = self.min
        end = self.max
        return 'TimeBubble({}, {})'.format(repr(start), repr(end))

    def __str__(self):
        fmt = '%Y-%m-%d'
        start = self.min.strftime(fmt)
        end = self.max.strftime(fmt)
        return 'TimeBubble({}, {})'.format(start, end)

    def __contains__(self, value):
        try:
            return self.min <= value <= self.max
        except:
            return self.min <= value.min < value.max <= self.max

    def to_list(self):
        return [self.min, self.max]

    def merge(self, bubble):
        if self._mergeable(bubble):
            mi = min(self.min, bubble.min)
            ma = max(self.max, bubble.max)
            return self.__class__(mi, ma), None
        else:
            return self, bubble

    def carve(self, bubble):
        if self._mergeable(bubble):
            l = self.min
            u = bubble.min - timedelta(1)
            left = self.__class__(l, u) if l < u else None
            l = bubble.max + timedelta(1)
            u = self.max
            right = self.__class__(l, u) if l < u else None
            return left, right
        else:
            return self, None

    def _mergeable(self, bubble):
        return abs(self.mid - bubble.mid) <= (self.leg + bubble.leg)


class Bubbles():
    def __init__(self, bubbles=[]):
        self.bubbles = []
        for b in (self._convert(b) for b in bubbles):
            self.bubbles.append(b)
        self._sort()
        self._squeeze()

    def __repr__(self):
        return 'Bubbles({})'.format(self.bubbles)

    def __str__(self):
        return self.__repr__()

    def __contains__(self, value):
        for b in self.bubbles:
            if value in b:
                return True
        return False

    def to_list(self):
        res = []
        for b in self.bubbles:
            res.append(b.to_list())
        return res

    def merge(self, bubble):
        self.bubbles.append(self._convert(bubble))
        self._sort()
        self._squeeze()
        return self

    def carve(self, bubble):
        b_carve = self._convert(bubble)
        res = []
        for b in self.bubbles:
            b1, b2 = b.carve(b_carve)
            res.append(b1)
            res.append(b2)

        self.bubbles = res
        self._dropnone()
        self._sort()
        self._squeeze()
        return self

    def _squeeze(self):
        p = 0
        count = len(self.bubbles)
        while p < count:
            for q, b_q in ((q, b_q) for q, b_q in enumerate(self.bubbles) if q > p):
                self.bubbles[p], self.bubbles[q] = self.bubbles[p].merge(b_q)
            self._dropnone()
            p += 1
            count = len(self.bubbles)

    def _sort(self):
        self.bubbles.sort(key=lambda x: x.mid)

    def _dropnone(self):
        self.bubbles = list(filter(lambda x: not x is None, self.bubbles))

    def _convert(self, limits):
        return TimeBubble(min(limits), max(limits))
