import time
import logging

import pybloom

class ExpiringBloomFilter():
    def __init__(self, expiry_time = 3600*24, n_filter = 10, capacity = 10e7):
        self.n_filter = n_filter
        self.expiry_time = expiry_time
        self.resolution = self.expiry_time / self.n_filter
        self.capacity = capacity
        self.__filters = [pybloom.pybloom.BloomFilter(self.capacity) for i in range(n_filter)]
        self.last_tick = 0
        self.__global_filter = pybloom.pybloom.BloomFilter(self.capacity)

    # When we lookup, just check the global filter
    def __contains__(self, item):
        return item in self.__global_filter

    def add(self, item):
        # Get the current time
        t = int(time.time())

        # Tick
        self.tick(t)

        # Get the new time bucket index
        bucket_index = (t // self.resolution) % self.n_filter

        # Add the item to this, and the global filter
        self.__global_filter.add(item)
        self.__filters[bucket_index].add(item)

    def tick(self, t):
        # Decide if any action is required
        if t < self.last_tick + self.resolution:
            # Nothing to do if we ticked already in this time bucket
            return


        # Get the new time bucket index
        bucket_index = (t // self.resolution) % self.n_filter
        last_expired_index = (self.last_tick // self.resolution) % self.n_filter

        # Age off the data in that bucket, and any leading up to it
        expire_index = last_expired_index
        while expire_index != bucket_index:
            expire_index = (expire_index + 1) % self.n_filter
            logging.info('Expiring bucket %d' % expire_index)
            self.__filters[expire_index] = pybloom.pybloom.BloomFilter(self.capacity)

        self.__filters[bucket_index] = pybloom.pybloom.BloomFilter(self.capacity)

        # Update the global filter (a combination of all filters)
        self.__global_filter = pybloom.pybloom.BloomFilter(self.capacity)
        for bf in self.__filters:
            self.__global_filter = self.__global_filter.union(bf)

        # Remeber we ticked for next time
        self.last_tick = t
