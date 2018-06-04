#!/usr/bin/env python

import sys

airports_dict = {}

airports = open('airports.csv','r')

for line in airports:
   line = line.strip()
   splits = line.split(",")
   airports_dict[splits[0]] = splits[1]

airports.close()

for line in sys.stdin:
    line = line.strip()
    splits = line.split(",")
    if (splits[8] in airports_dict):
        splits.insert(9,airports_dict[splits[8]])
    else:
        splits.insert(9,'NA')
    print(','.join(splits))
