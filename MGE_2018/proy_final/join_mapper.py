#!/usr/bin/env python

import sys
for line in sys.stdin:
   line = line.strip()
   splits = line.split(",")

   if len(splits) < 3:
        mes_key = splits[0]+"001"
        value =splits[1]
   else:
        mes_key = splits[15]+"002"
        value = "-"

   print(mes_key +"|"+value)
