from datetime import datetime
from datetime import timedelta


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
        self.bubbles = []
        for b in (self._convert(b) for b in bubbles):
            self.bubbles.append(b)
        self._triple_kill()

    def __repr__(self):
        return 'Bubbles({})'.format(self.bubbles)

    def __str__(self):
        return 'Bubbles({})'.format(str(self.bubbles))

    def __contains__(self, value):
        for b in self.bubbles:
            if value in b:
                return True
        return False

    def __len__(self):
        return len(self.bubbles)

    def __getitem__(self, key):
        return self.bubbles[key]

    def __setitem__(self, key, value):
        if isinstance(value, TimeBubble):
            self.bubbles[key] = value
        else:
            raise ValueError(
                'Value {} not compatible with Bubbles.'.format(value))
        self._triple_kill()

    def __delitem__(self, key):
        del self.bubbles[key]

    def __iter__(self):
        return iter(self.bubbles)

    @property
    def min(self):
        return self.bubbles[0].min

    @property
    def max(self):
        return self.bubbles[-1].max

    def gaps(self, bubble=None):
        '''Return gaps between bubbles.'''
        fullbubble = TimeBubble(self.min, self.max) \
            if bubble is None else self._convert(bubble)

        res = Bubbles()

        try:
            for b in self.bubbles:
                l, r = fullbubble.carve(b)
                fullbubble = r
                res.bubbles.append(l)
        except AttributeError:
            pass
        finally:
            res.bubbles.append(r)
            res._triple_kill()
            return res

    def to_list(self):
        res = []
        for b in self.bubbles:
            res.append(b.to_list())
        return res

    def merge(self, bubble):
        self.bubbles.append(self._convert(bubble))
        self._triple_kill()
        return self

    def carve(self, bubble):
        b_carve = self._convert(bubble)
        res = []
        for b in self.bubbles:
            b1, b2 = b.carve(b_carve)
            res.append(b1)
            res.append(b2)

        self.bubbles = res
        self._triple_kill()
        return self

    def _triple_kill(self):
        self._drop_none()
        self._sort()
        self._squeeze()

    def _squeeze(self):
        p = 0
        count = len(self.bubbles)
        while p < count:
            for q, b_q in ((q, b_q) for q, b_q in enumerate(self.bubbles) if q > p):
                self.bubbles[p], self.bubbles[q] = self.bubbles[p].merge(b_q)
            self._drop_none()
            p += 1
            count = len(self.bubbles)

    def _sort(self):
        self.bubbles.sort(key=lambda x: x.mid)

    def _drop_none(self):
        self.bubbles = [b for b in self.bubbles if b is not None]

    def _convert(self, limits):
        return TimeBubble(min(limits), max(limits))
