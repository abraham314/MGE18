#!/usr/bin/python

import sys
for line in sys.stdin:
   line = line.strip()
   splits = line.split(",")

   if len(splits) > 30:
        airline_key = splits.pop(11) + '_2'
        value = ','.join(splits)
   else:
        airline_key = splits.pop(0) + '_1'
        value = ','.join(splits)

   print(airline_key + '|' + value)