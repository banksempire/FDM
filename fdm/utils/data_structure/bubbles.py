from datetime import datetime
from datetime import timedelta

from copy import deepcopy


class TimeBubble:
    def __init__(self, lower, upper, delta=timedelta(days=1)):
        if lower > upper:
            raise ValueError(
                'Upper {} is less then lower {}.'.format(upper, lower))
        self.max = upper
        self.min = lower
        self.mid = self.max.timestamp() + self.min.timestamp()
        self.leg = self.max.timestamp() - self.min.timestamp()
        self.delta = delta

    def __repr__(self):
        start = self.min
        end = self.max
        return 'TimeBubble({}, {})'.format(repr(start), repr(end))

    def __str__(self):
        fmt = '%Y-%m-%d'
        start = self.min.strftime(fmt)
        end = self.max.strftime(fmt)
        return '[{}, {})'.format(start, end)

    def __contains__(self, value):
        try:
            return self.min <= value < self.max
        except:
            return self.min <= value.min <= value.max < self.max

    def to_list(self):
        return [self.min, self.max]

    def to_timerange(self):
        return [self.min, self.max-self.delta]

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
            u = bubble.min
            left = self.__class__(l, u) if l < u else None
            l = bubble.max
            u = self.max
            right = self.__class__(l, u) if l < u else None
            return left, right
        else:
            return self, None

    def _mergeable(self, bubble):
        return abs(self.mid - bubble.mid) <= (self.leg + bubble.leg)


class Bubbles():
    def __init__(self, bubbles=[]):
        self._bubbles = []
        for b in (self._convert(b) for b in bubbles):
            self._bubbles.append(b)
        self._triple_kill()

    def __repr__(self):
        return 'Bubbles({})'.format(self._bubbles)

    def __str__(self):
        return 'Bubbles({})'.format(str(self._bubbles))

    def __contains__(self, value):
        for b in self._bubbles:
            if value in b:
                return True
        return False

    def __len__(self):
        return len(self._bubbles)

    def __getitem__(self, key):
        return self._bubbles[key]

    def __setitem__(self, key, value):
        try:
            self._bubbles[key] = self._convert(value)
        except:
            raise ValueError(
                'Value {} not compatible with Bubbles.'.format(value))
        self._triple_kill()

    def __delitem__(self, key):
        del self._bubbles[key]

    def __iter__(self):
        return iter(self._bubbles)

    @property
    def min(self):
        return self._bubbles[0].min

    @property
    def max(self):
        return self._bubbles[-1].max

    def gaps(self, bubble=None):
        '''Return gaps between bubbles.'''
        fullbubble = TimeBubble(self.min, self.max) \
            if bubble is None else self._convert(bubble)

        res = Bubbles()

        try:
            for b in self._bubbles:
                l, r = fullbubble.carve(b)
                fullbubble = r
                res._bubbles.append(l)
        except AttributeError:
            pass
        finally:
            res._bubbles.append(r)
            res._triple_kill()
            return res

    def to_list(self):
        res = []
        for b in self._bubbles:
            res.append(b.to_list())
        return res

    def merge(self, bubble):
        res = Bubbles()
        res._bubbles = deepcopy(self._bubbles)
        res._bubbles.append(res._convert(bubble))
        res._triple_kill()
        return res

    def carve(self, bubble):
        b_carve = self._convert(bubble)
        res_list = []
        for b in self._bubbles:
            b1, b2 = b.carve(b_carve)
            res_list.append(b1)
            res_list.append(b2)

        return Bubbles(res_list)

    def _triple_kill(self):
        self._drop_none()
        self._sort()
        self._squeeze()

    def _squeeze(self):
        p = 0
        count = len(self._bubbles)
        while p < count:
            for q, b_q in ((q, b_q) for q, b_q in enumerate(self._bubbles) if q > p):
                self._bubbles[p], self._bubbles[q] = self._bubbles[p].merge(
                    b_q)
            self._drop_none()
            p += 1
            count = len(self._bubbles)

    def _sort(self):
        self._bubbles.sort(key=lambda x: x.mid)

    def _drop_none(self):
        self._bubbles = [b for b in self._bubbles if b is not None]

    def _convert(self, value):
        if isinstance(value, TimeBubble):
            return value
        elif value is None:
            return None
        else:
            return TimeBubble(min(value), max(value))
