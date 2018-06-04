#!/usr/bin/env python

import sys
for line in sys.stdin:
   line = line.strip()
   splits = line.split(",")

   if len(splits) > 30:
        airport_key = splits.pop(7) + '_2'
        value = ','.join(splits)
   else:
        airport_key = splits.pop(0) + '_1'
        value = ','.join(splits)

   print(airport_key + '|' + value)
