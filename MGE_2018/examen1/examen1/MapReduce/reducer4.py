#!/usr/bin/python

import sys

previous = None
sum = 0
miss = 0 

for line in sys.stdin:
  key, value,value2 = line.split('\t') 

  if key != previous:
      if previous is not None:
        print(previous + '\t' + str(sum)+'\t'+str(miss))
      previous = key
      sum = 0
      miss = 0 

  sum += int(value)
  miss += int(value2)

print(previous + '\t' + str(sum)+'\t'+str(miss))
