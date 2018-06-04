#!/usr/bin/env python

import sys

airline_dict = {}

airlines = open('airlines.csv','r')

for line in airlines:
   line = line.strip()
   splits = line.split(",")
   airline_dict[splits[0]] = splits[1]

airlines.close()

for line in sys.stdin:
    line = line.strip()
    splits = line.split(",")
    if (splits[4] in airline_dict):
        splits.insert(5,airline_dict[splits[4]])
    else:
        splits.insert(5,'NA')
    print(','.join(splits))
