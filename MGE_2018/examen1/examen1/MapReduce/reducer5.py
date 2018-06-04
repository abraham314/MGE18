#!/usr/bin/python

import sys

previous = None
sum = 0
miss = 0 

for line in sys.stdin:
  key, value,value2 = line.split('\t') 

  if key != previous:
      if previous in ("ACO","ATI","CHO","CUT","FAC","INN","LPR","LLA","MON","NEZ","SAG","TLA","TLI","VIF","XAL"):
        print(previous + '\t' + str(sum)+'\t'+str(miss))
      previous = key
      sum = 0
      miss = 0 

  sum += int(value)
  miss += int(value2)

if previous in ("ACO","ATI","CHO","CUT","FAC","INN","LPR","LLA","MON","NEZ","SAG","TLA","TLI","VIF","XAL"):
   print(previous + '\t' + str(sum)+'\t'+str(miss))
