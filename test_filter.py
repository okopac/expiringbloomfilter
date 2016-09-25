import ExpiringBloomFilter
import time

ebf = ExpiringBloomFilter.ExpiringBloomFilter(expiry_time = 10, capacity = 100)

while True:
    for i in range(10):
        print i, [j in ebf for j in range(10)]
        ebf.add(i)
        time.sleep(2)
