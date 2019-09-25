class Bubble:
    def __init__(self, lower, upper):
        if lower > upper:
            raise ValueError(
                'Upper {} is less then lower {}.'.format(upper, lower))
        self.max = upper
        self.min = lower
        self.mid = self.max + self.min
        self.leg = self.max - self.min

    def __repr__(self):
        return 'Bubble(lower={}, upper={})'.format(self.min, self.max)

    def __str__(self):
        return self.__repr__()

    def __contains__(self, value):
        return self.min <= value <= self.max

    def merge(self, bubble):
        if self._mergeable(bubble):
            mi = min(self.min, bubble.min)
            ma = max(self.max, bubble.max)
            return Bubble(mi, ma), None
        else:
            return self, bubble

    def carve(self, bubble):
        if self._mergeable(bubble):
            l = self.min
            u = bubble.min
            left = Bubble(l, u) if l < u else None
            l = bubble.max
            u = self.max
            right = Bubble(l, u) if l < u else None
            return left, right
        else:
            return self, None

    def _mergeable(self, bubble):
        return abs(self.mid - bubble.mid) <= (self.leg + bubble.leg)


class Bubbles():
    def __init__(self):
        self.bubbles = []

    def append(self, bubble):
        self.bubbles.append(bubble)
        self.bubbles.sort(key=lambda x: x.mid)
        count = len(self.bubbles)
        bubbles = [(False, bubble)]
        for i in range(count-1):
            l, r = self.bubbles[i].merge(self.bubbles[i+1])
            if r is None:  # if merge success
                pass
